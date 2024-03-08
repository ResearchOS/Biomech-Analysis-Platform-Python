import weakref
from typing import Any
import json, re
from typing import TYPE_CHECKING
import sqlite3
from hashlib import sha256
import pickle
from datetime import datetime, timezone
import logging, time, sys
import copy

# from inspect_locals import inspect_locals

import numpy as np
# from memory_profiler import profile

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject
    # from ResearchOS.DataObjects.data_object import DataObject
    # from ResearchOS.variable import Variable

# from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.action import Action
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.sql.sql_joiner_most_recent import sql_joiner_most_recent
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.current_user import CurrentUser
from ResearchOS.get_computer_id import COMPUTER_ID

# set_vr_values_log = open("logfile_set_vr_values.log", "w")

do_run = False

class ResearchObjectHandler:
    """Keep track of all instances of all research objects. This is an static class."""

    instances = weakref.WeakValueDictionary() # Keep track of all instances of all research objects.
    # instances = {} # Keep track of all instances of all research objects.
    counts = {} # Keep track of the number of instances of each ID.    
    pool = SQLiteConnectionPool(name = "main")
    pool_data = SQLiteConnectionPool(name = "data")
    default_attrs = {} # Keep track of the default attributes for each class.    

    # @staticmethod
    # def load_vr_value(research_object: "DataObject", action: Action, vr: "Variable") -> Any:
    #     """Load the value of the variable for this DataObject."""
    #     # 1. Get the latest data_blob_id.
    #     dataset_id = research_object.get_dataset_id()        
    #     schema_id = research_object.get_current_schema_id(dataset_id)
    #     conn = action.conn
    #     cursor = conn.cursor()
    #     sqlquery_raw = f"SELECT action_id, data_blob_hash FROM data_values WHERE vr_id = '{vr.id}' AND dataobject_id = '{research_object.id}' AND schema_id = '{schema_id}'"
    #     sqlquery = sql_order_result(action, sqlquery_raw, ["vr_id", "dataobject_id", "schema_id"], single = True, user = True, computer = False)
    #     time_ordered_result = cursor.execute(sqlquery).fetchall()
    #     ResearchObjectHandler.pool.return_connection(conn)
    #     if len(time_ordered_result) == 0:
    #         raise ValueError("No value exists for that VR for this DataObject.")
    #         # return "Missing"              
    #     data_blob_id = time_ordered_result[0][1]

    #     # 2. Get the data_blob from the data_blobs table.
    #     conn_data = ResearchObjectHandler.pool_data.get_connection()
    #     cursor_data = conn_data.cursor()
    #     sqlquery = "SELECT data_blob FROM data_values_blob WHERE data_blob_hash = ?"        
    #     params = (data_blob_id,)
    #     rows = cursor_data.execute(sqlquery, params).fetchall()
    #     ResearchObjectHandler.pool_data.return_connection(conn_data)
    #     return pickle.loads(rows[0][0])     

    @staticmethod
    def from_json(research_object: "ResearchObject", attr_name: str, attr_value_json: Any, action: Action = None) -> Any:
        """Convert the JSON string to an attribute value. If there is no class-specific way to do it, then use the builtin json.loads"""
        try:
            from_json_method = getattr(research_object, "from_json_" + attr_name)
            attr_value = from_json_method(attr_value_json, action)
        except AttributeError as e:
            attr_value = json.loads(attr_value_json)
        return attr_value
    
    @staticmethod
    def object_exists(id: str, action: Action) -> bool:
        """Return true if the specified id exists in the database, false if not."""
        # if id in ResearchObjectHandler.instances:
        #     return True
        cursor = action.conn.cursor()
        sqlquery = "SELECT object_id FROM research_objects WHERE object_id = ?"
        cursor.execute(sqlquery, (id,))
        rows = cursor.fetchone()
        return rows is not None               

    @staticmethod
    def _get_most_recent_attrs(research_object: "ResearchObject", ordered_attr_result: list, default_attrs: dict = {}, action: Action = None) -> dict:
        """Given a list of all attributes for this object, return the most recent attributes.
        NOTE: Does NOT validate the attributes here."""
        curr_obj_attr_ids = [row[1] for row in ordered_attr_result]
        num_attrs = len(list(set(curr_obj_attr_ids))) # Get the number of unique action ID's.
        used_attr_ids = []
        attrs = {}
        for row in ordered_attr_result:            
            attr_id = row[1]
            attr_value_json = row[2]
            # target_object_id = row[3]
            if attr_id in used_attr_ids:
                continue
            
            used_attr_ids.append(attr_id)                        
            attr_name = ResearchObjectHandler._get_attr_name(attr_id)            
            attr_value = ResearchObjectHandler.from_json(research_object, attr_name, attr_value_json) # Translate the attribute from string to the proper type/format.
                        
            attrs[attr_name] = attr_value
            if len(used_attr_ids) == num_attrs:
                break # Every attribute is accounted for.
        return attrs

    @staticmethod
    def _load_ro(research_object: "ResearchObject", all_attrs: dict, action: Action) -> None:
        """Load "simple" attributes from the database."""
        default_attrs = all_attrs.default_attrs
        computer_specific_attr_names = all_attrs.computer_specific_attr_names
        # 1. Get the database cursor.
        conn = action.conn
        cursor = conn.cursor()

        # 2. Get the attributes from the database.
        sqlquery_raw = "SELECT attr_id, attr_value FROM simple_attributes WHERE object_id = ?"
        unique_cols = ["object_id", "attr_id"]
        sqlquery = sql_order_result(action, sqlquery_raw, unique_cols, single = True, user = True, computer = False)        
        params = (research_object.id,)
        ordered_attr_result_all_computers = cursor.execute(sqlquery, params).fetchall()
        sqlquery = sql_order_result(action, sqlquery_raw, unique_cols, single = True, user = True, computer = True)
        ordered_attr_result_computer = cursor.execute(sqlquery, params).fetchall()
        ordered_attr_result_all_computers_dict = {row[0]: row[1] for row in ordered_attr_result_all_computers}
        ordered_attr_result_computer_dict = {row[0]: row[1] for row in ordered_attr_result_computer}

        ordered_attr_result_dict = ordered_attr_result_all_computers_dict | ordered_attr_result_computer_dict # Current computer dict takes precedence.

        if not ordered_attr_result_all_computers:
            raise ValueError("No object with that ID exists.")
        
        if not ordered_attr_result_computer and computer_specific_attr_names:
            raise ValueError("No computer-specific attributes exist for this object.")

        # Computer-specific and computer-independent attributes.
        attrs = {}
        for attr_id, value in ordered_attr_result_dict.items():            
            attr_name = ResearchObjectHandler._get_attr_name(attr_id)
            if hasattr(research_object, "load_" + attr_name):
                load_method = getattr(research_object, "load_" + attr_name)
                attr_value = load_method(action)
            else:
                attr_value = ResearchObjectHandler.from_json(research_object, attr_name, value, action)
            attrs[attr_name] = attr_value

        # 3. Set the attributes of the object.
        research_object.__dict__.update(attrs)

        # 4. Load the class-specific/"complex" builtin attributes.
        for attr_name in default_attrs.keys():
            if hasattr(research_object, "load_" + attr_name):
                load_method = getattr(research_object, "load_" + attr_name)
                value = load_method(action)
                research_object.__dict__[attr_name] = value

        # 5. Validate the simple & complex builtin attributes.
        for default_attr_name, default_attr_value in default_attrs.items():
            curr_value = research_object.__dict__[default_attr_name]
            ResearchObjectHandler.validate(research_object, default_attr_name, curr_value, action, default_attr_value)

        # 6. Load the VR attributes. No validation is necessary here.
        # dobj_subclasses = DataObject.__subclasses__()
        # if research_object.__class__ in dobj_subclasses:
        #     research_object.load_dataobject_vrs(action = action)

    @staticmethod
    def validate(research_object: "ResearchObject", name: str, value: Any, action: Action, default: Any) -> None:
        """Validate the value of the attribute."""        
        if not hasattr(research_object, "validate_" + name):
            return
                
        validate_method = getattr(research_object, "validate_" + name)
        validate_method(value, action, default)

    @staticmethod
    def _set_builtin_attributes(research_object: "ResearchObject", default_attrs: dict, kwargs: dict, action: Action):
        """Responsible for setting the value of all builtin attributes, simple or not."""  

        # !. Set simple builtin attributes.
        complex_attrs = {}
        for key in kwargs:

            if key not in default_attrs:
                continue # Skip the attribute if it is not a default attribute.

            # 1. Skip the complex attributes.
            if hasattr(research_object, "save_" + key):
                complex_attrs[key] = kwargs[key]
                continue # Complex attributes are set in the next step.

            # 2. If previously initialized, skip the attribute if it was previously loaded and the value has not changed (even if it was a kwarg).
            if research_object._initialized and key in research_object.__dict__ and getattr(research_object, key) == kwargs[key]:
                continue

            # 3. Validate the attribute.
            ResearchObjectHandler.validate(research_object, key, kwargs[key], action, default_attrs[key])

            if hasattr(research_object, "to_json_" + key):
                to_json_method = getattr(research_object, "to_json_" + key)
                json_value = to_json_method(kwargs[key], action)
            else:
                json_value = json.dumps(kwargs[key])  

            simple_params = (action.id, research_object.id, ResearchObjectHandler._get_attr_id(key), json_value)
            action.add_sql_query(research_object.id, "robj_simple_attr_insert", simple_params, group_name = "robj_simple_attr_insert")

            # 4. Get the parameters for the SQL query to set the attribute.            
            research_object.__dict__[key] = kwargs[key] # Set the attribute in the object's __dict__.        

        # 2. Set complex builtin attributes.
        for key in complex_attrs:

            # 1. Skip the attribute if it was previously loaded and the value has not changed (even if it was a kwarg).
            if key in research_object.__dict__ and getattr(research_object, key) == complex_attrs[key]:
                continue

            # 2. Validate the attribute.
            ResearchObjectHandler.validate(research_object, key, complex_attrs[key], action, default_attrs[key])

            # 3.  Save the "complex" builtin attribute to the database.
            save_method = getattr(research_object, "save_" + key)
            save_method(complex_attrs[key], action = action)

            research_object.__dict__[key] = complex_attrs[key]

    @staticmethod
    # @profile(stream = set_vr_values_log)
    def _set_vr_values(research_object: "ResearchObject", vr_values: dict, action: Action, pr_id: str) -> None:
        """Set the values of the VR attributes."""
        if not vr_values:
            return
        # 1. Get hash of each value.
        start_pickle_hash_time = time.time()
        logging.info(f"Setting VR values for {research_object.id}.")
        vr_hashes_dict = {}
        for vr, value in vr_values.items():
            # time.sleep(0.01)
            start_pickle_time = time.time()
            data_blob = pickle.dumps(value, protocol = 4)
            pickle_dur = time.time() - start_pickle_time
            start_hash_time = time.time()
            data_blob_hash = sha256(data_blob).hexdigest()
            hash_dur = time.time() - start_hash_time
            vr_hashes_dict[vr] = {"hash": data_blob_hash, "blob": data_blob}

            if pickle_dur > 2 or hash_dur > 2:
                logging.warning(f"Time to pickle {vr.id} ({vr.name}): {pickle_dur} seconds.")
                logging.warning(f"Time to hash {vr.id} ({vr.name}): {hash_dur} seconds.")
            else:
                logging.debug(f"Time to pickle {vr.id} ({vr.name}): {pickle_dur} seconds.")
                logging.debug(f"Time to hash {vr.id} ({vr.name}): {hash_dur} seconds.")

            vr_values[vr] = data_blob_hash # Replace the value with the hash to relieve memory pressure.            
        logging.debug(f"Time to pickle and hash VR output values: {time.time() - start_pickle_hash_time} seconds.")

        # 2. Check which VR's hashes are already in the data database so as not to duplicate a primary key.
        pool_data = SQLiteConnectionPool(name = "data")
        conn_data = pool_data.get_connection()
        cursor_data = conn_data.cursor()
        vr_hashes = tuple(set([vr["hash"] for vr in vr_hashes_dict.values()])) # Get unique values.
        sqlquery = "SELECT data_blob_hash FROM data_values_blob WHERE data_blob_hash IN ({})".format(", ".join("?" * len(vr_hashes)))
        result = cursor_data.execute(sqlquery, vr_hashes).fetchall()
        pool_data.return_connection(conn_data)
        vr_hashes_prev_exist = []        
        for vr in vr_hashes_dict:
            for row in result:
                hash = row[0]
                if hash == vr_hashes_dict[vr]["hash"]:
                    vr_hashes_prev_exist.append(vr)
                    break    

        # 2. Insert the values into the proper tables.
        schema_id = research_object.get_current_schema_id(research_object.get_dataset_id())
        for vr in vr_hashes_dict:
            blob_params = (vr_hashes_dict[vr]["hash"], vr_hashes_dict[vr]["blob"])
            blob_pk = blob_params
            vr_dobj_params = (action.id, research_object.id, vr.id)
            vr_dobj_pk = vr_dobj_params
            vr_value_params = (action.id, vr.id, research_object.id, schema_id, vr_hashes_dict[vr]["hash"], pr_id)
            vr_value_pk = vr_value_params
            # Don't insert the data_blob if it already exists.
            if not vr in vr_hashes_prev_exist:
                if not action.is_redundant_params(research_object.id, "data_value_in_blob_insert", blob_pk, group_name = "robj_vr_attr_insert"):
                    action.add_sql_query(research_object.id, "data_value_in_blob_insert", blob_params, group_name = "robj_vr_attr_insert")
            # No danger of duplicating primary keys, so no need to check if they previously existed.
            if not action.is_redundant_params(research_object.id, "vr_to_dobj_insert", vr_dobj_pk, group_name = "robj_vr_attr_insert"):
                action.add_sql_query(research_object.id, "vr_to_dobj_insert", vr_dobj_params, group_name = "robj_vr_attr_insert")
            if not action.is_redundant_params(research_object.id, "vr_value_for_dobj_insert", vr_value_pk, group_name = "robj_vr_attr_insert"):
                action.add_sql_query(research_object.id, "vr_value_for_dobj_insert", vr_value_params, group_name = "robj_vr_attr_insert")


    @staticmethod
    def clean_value_from_load_mat(numpy_array: Any) -> Any:
        """Ensure that all data types are from scipy.io.loadmat are Pythonic."""
        if isinstance(numpy_array, np.ndarray) and numpy_array.dtype.names is not None:
            return {name: ResearchObjectHandler.clean_value_from_load_mat(numpy_array[name]) for name in numpy_array.dtype.names}
        elif isinstance(numpy_array, np.ndarray):
            numpy_array_tmp = numpy_array
            if numpy_array.size == 1:
                numpy_array_tmp = numpy_array[0]
            return numpy_array_tmp.tolist()
        else:
            return numpy_array

    @staticmethod
    def clean_value_for_save_mat(value: Any) -> Any:
        """Ensure that all data types are compatible with the scipy.io.savemat method."""
        # Recursive.
        if isinstance(value, (dict, set)):
            for key in value.keys():
                value[key] = ResearchObjectHandler.clean_value_for_save_mat(value[key])
            return value

        if isinstance(value, (list, tuple)):
            if len(value) == 0:
                return value
            if isinstance(value[0], (list, tuple, dict, set)):
                for i, item in enumerate(value):
                    value[i] = ResearchObjectHandler.clean_value_for_save_mat(item)
                return value

        # Actually clean the values.
        return np.array(value)
    
    @staticmethod
    def save_simple_attribute(id: str, name: str, json_value: Any, action: Action) -> Action:
        """If no store_attr method exists for the object attribute, use this default method."""                                      
        sqlquery = "INSERT INTO simple_attributes (action_id, object_id, attr_id, attr_value) VALUES (?, ?, ?, ?)"
        params = (action.id, id, ResearchObjectHandler._get_attr_id(name), json_value)
        action.add_sql_query(sqlquery, params)
        action.add_params(params)
    
    @staticmethod
    def _get_attr_name(attr_id: int) -> str:
        """Get the name of an attribute given the attribute's ID. If it does not exist, return an error."""
        # cursor = DBConnectionFactory.create_db_connection().conn.cursor()
        conn = ResearchObjectHandler.pool.get_connection()
        cursor = conn.cursor()
        sqlquery = f"SELECT attr_name FROM attributes_list WHERE attr_id = '{attr_id}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) == 0:
            raise Exception("No attribute with that ID exists.")
        ResearchObjectHandler.pool.return_connection(conn)
        return rows[0][0]  
    
    @staticmethod
    def _get_attr_id(attr_name: str) -> str:
        """Get the ID of the attribute."""
        pool = SQLiteConnectionPool()
        conn = pool.get_connection()
        cursor = conn.cursor()
        sqlquery = f"SELECT attr_id FROM attributes_list WHERE attr_name = '{attr_name}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) > 0:
            attr_id = rows[0][0]
        else:
            sqlquery = f"INSERT INTO attributes_list (attr_name) VALUES ('{attr_name}')"
            try:
                cursor.execute(sqlquery)
                conn.commit()
                attr_id = cursor.lastrowid
            except sqlite3.Error as e:
                print(e)
                raise e
        pool.return_connection(conn)
        return attr_id
        
    @staticmethod
    def _get_time_ordered_result(result: list, action_col_num: int) -> list:
        """Return the result array from conn.cursor().execute() in reverse chronological order (e.g. latest first)."""
        unordered_action_ids = [row[action_col_num] for row in result] # A list of action ID's in no particular order.
        action_ids_str = ', '.join([f"'{action_id}'" for action_id in unordered_action_ids])
        sqlquery = f"SELECT action_id FROM actions WHERE action_id IN ({action_ids_str}) ORDER BY datetime DESC"
        conn = ResearchObjectHandler.pool.get_connection()
        cursor = conn.cursor()
        ordered_action_ids = cursor.execute(sqlquery).fetchall()
        ResearchObjectHandler.pool.return_connection(conn)
        if ordered_action_ids is None or len(ordered_action_ids) == 0:
            return ordered_action_ids # Sometimes it's ok that there's no "simple" attributes?
        ordered_action_ids = [action_id[0] for action_id in ordered_action_ids]
        indices = []
        for action_id in ordered_action_ids:
            for i, unordered_action_id in enumerate(unordered_action_ids):
                if unordered_action_id == action_id:
                    indices.append(i)
        sorted_result = [result[index] for index in indices]        
        return sorted_result
    
    @staticmethod
    def _get_subclasses(cls):
        """Recursively get all subclasses of the provided class."""        
        subclasses = cls.__subclasses__()
        result = subclasses[:]
        for subclass in subclasses:
            result.extend(ResearchObjectHandler._get_subclasses(subclass))
        return result
    
    @staticmethod
    def _prefix_to_class(prefix: str) -> type:
        """Convert a prefix to a class."""
        from ResearchOS.research_object import ResearchObject
        for cls in ResearchObjectHandler._get_subclasses(ResearchObject):
            if hasattr(cls, "prefix") and prefix.startswith(cls.prefix):
                return cls
        raise ValueError("No class with that prefix exists.")
    
    @staticmethod
    def _set_simple_builtin_attr(research_object, name: str, value: Any, action: Action = None) -> None:
        """Set the attributes of a research object in memory and in the SQL database.
        Validates the attribute if it is a built-in ResearchOS attribute (i.e. a method exists to validate it).
        If it is not a built-in ResearchOS attribute, then no validation occurs."""
        # TODO: Have already implemented adding current_XXX_id object to digraph in the database, but should also update the in-memory digraph.     
        
        if hasattr(research_object, "to_json_" + name):
            to_json_method = getattr(research_object, "to_json_" + name)
            json_value = to_json_method(value, action)
        else:
            json_value = json.dumps(value)   

        ResearchObjectHandler.save_simple_attribute(research_object.id, name, json_value, action = action)            
        research_object.__dict__[name] = value

    @staticmethod
    def get_user_computer_path(research_object, attr_name: str, action: Action) -> str:
        """Get a user- and computer-specific path, which is a simple attribute in the database."""
        # Load the most recent path.
        attr_id = ResearchObjectHandler._get_attr_id(attr_name)
        sqlquery_raw = "SELECT action_id, attr_value FROM simple_attributes WHERE attr_id = ? AND object_id = ?"
        sqlquery = sql_order_result(action, sqlquery_raw, ["attr_id", "object_id"], single = True, user = True, computer = True)
        params = (attr_id, research_object.id)
        result = action.conn.execute(sqlquery, params).fetchall()
        if not result:
            return None
        path = result[0][1]
        path = json.loads(path)

        # Get the timestamp of this action_id
        sqlquery = "SELECT datetime FROM actions WHERE action_id = ?"
        params = (result[0][0],)
        timestamp_path = action.conn.execute(sqlquery, params).fetchone()[0]
        
        # Check if the action_id is after the current user/computer action_id.
        sqlquery_raw = "SELECT action_id FROM users_computers WHERE user_id = ? AND computer_id = ?"
        # sqlquery = sql_order_result(action, sqlquery_raw, ["user_id", "computer_id"], single = True, user = False, computer = False)
        sqlquery = sql_joiner_most_recent(sqlquery_raw)
        params = (CurrentUser.current_user, COMPUTER_ID)
        action_id_users = action.conn.execute(sqlquery, params).fetchone()[0]

        # Get the timestamp for when the user ID was set.
        sqlquery = "SELECT datetime FROM actions WHERE action_id = ?"
        params = (action_id_users,)
        timestamp_users = action.conn.execute(sqlquery, params).fetchone()[0]

        timestamp_path = datetime.strptime(timestamp_path, "%Y-%m-%d %H:%M:%S.%f%z").replace(tzinfo=timezone.utc)
        timestamp_users = datetime.strptime(timestamp_users, "%Y-%m-%d %H:%M:%S.%f%z").replace(tzinfo=timezone.utc)

        if timestamp_path < timestamp_users:
            return None
        return path    
