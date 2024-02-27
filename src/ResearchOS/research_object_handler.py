import weakref
from typing import Any
import json, re
from typing import TYPE_CHECKING
import sqlite3
from hashlib import sha256
import pickle

import numpy as np

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject
    from ResearchOS.DataObjects.data_object import DataObject
    from ResearchOS.variable import Variable

# from ResearchOS.idcreator import IDCreator
from ResearchOS.action import Action
from ResearchOS.sqlite_pool import SQLiteConnectionPool

class ResearchObjectHandler:
    """Keep track of all instances of all research objects. This is an static class."""

    # instances = weakref.WeakValueDictionary() # Keep track of all instances of all research objects.
    instances = {} # Keep track of all instances of all research objects.
    counts = {} # Keep track of the number of instances of each ID.    
    pool = SQLiteConnectionPool(name = "main")
    pool_data = SQLiteConnectionPool(name = "data")
    default_attrs = {} # Keep track of the default attributes for each class.    

    @staticmethod
    def load_vr_value(research_object: "DataObject", vr: "Variable") -> Any:
        """Load the value of the variable for this DataObject."""
        # 1. Get the latest data_blob_id.
        dataset_id = research_object.get_dataset_id()        
        schema_id = research_object.get_current_schema_id(dataset_id)
        conn = ResearchObjectHandler.pool.get_connection()
        cursor = conn.cursor()
        sqlquery = f"SELECT action_id, data_blob_id FROM data_values WHERE vr_id = '{vr.id}' AND dataobject_id = '{research_object.id}' AND schema_id = '{schema_id}'"
        result = cursor.execute(sqlquery).fetchall()
        ResearchObjectHandler.pool.return_connection(conn)
        if len(result) == 0:
            raise ValueError("No value exists for that VR.")
        time_ordered_result = ResearchObjectHandler._get_time_ordered_result(result, action_col_num = 0)                
        data_blob_id = time_ordered_result[0][1]

        # 2. Get the data_blob from the data_blobs table.
        conn_data = ResearchObjectHandler.pool_data.get_connection()
        cursor_data = conn_data.cursor()
        sqlquery = "SELECT data_blob FROM data_values_blob WHERE data_blob_id = ?"        
        params = (data_blob_id,)
        rows = cursor_data.execute(sqlquery, params).fetchall()
        ResearchObjectHandler.pool_data.return_connection(conn_data)
        return pickle.loads(rows[0][0])     

    @staticmethod
    def from_json(research_object: "ResearchObject", attr_name: str, attr_value_json: Any, action: Action = None) -> Any:
        """Convert the JSON string to an attribute value. If there is no class-specific way to do it, then use the builtin json.loads"""
        try:
            from_json_method = eval("research_object.from_json_" + attr_name)
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
            
            # Now that the value is loaded as the proper type/format, validate it, if it is not the default value.
            if not (len(default_attrs) > 0 and attr_name in default_attrs and attr_value == default_attrs[attr_name]):
                ResearchObjectHandler.validate(research_object, attr_name, attr_value, action)
            attrs[attr_name] = attr_value
            if len(used_attr_ids) == num_attrs:
                break # Every attribute is accounted for.
        return attrs

    @staticmethod
    def _load_ro(research_object: "ResearchObject", default_attrs: dict, action: Action) -> None:
        """Load "simple" attributes from the database."""
        # 1. Get the database cursor.
        from ResearchOS.DataObjects.data_object import DataObject
        conn = ResearchObjectHandler.pool.get_connection()
        cursor = conn.cursor()

        # 2. Get the attributes from the database.
        sqlquery = f"SELECT action_id, attr_id, attr_value FROM simple_attributes WHERE object_id = '{research_object.id}'"
        unordered_attr_result = cursor.execute(sqlquery).fetchall()
        ResearchObjectHandler.pool.return_connection(conn)
        ordered_attr_result = ResearchObjectHandler._get_time_ordered_result(unordered_attr_result, action_col_num = 0)         
                             
        attrs = ResearchObjectHandler._get_most_recent_attrs(research_object, ordered_attr_result, default_attrs, action)   

        if attrs is None:
            raise ValueError("No object with that ID exists.")

        # 3. Set the attributes of the object.
        research_object.__dict__.update(attrs)

        # 4. Load the class-specific/"complex" builtin attributes.
        for key in default_attrs.keys():
            if key in research_object.__dict__:
                continue
            if hasattr(research_object, "load_" + key):
                load_method = getattr(research_object, "load_" + key)
                value = load_method(action)
                research_object.__dict__[key] = value

        dobj_subclasses = DataObject.__subclasses__()
        if research_object.__class__ in dobj_subclasses:
            research_object.load_dataobject_vrs()

    @staticmethod
    def validate(research_object: "ResearchObject", name: str, value: Any, action: Action) -> None:
        """Validate the value of the attribute."""
        if hasattr(research_object, "validate_" + name):
            validate_method = getattr(research_object, "validate_" + name)
            validate_method(value, action)

    @staticmethod
    def _set_builtin_attributes(research_object: "ResearchObject", default_attrs: dict, kwargs: dict, action: Action):
        """Responsible for setting the value of all builtin attributes, simple or not."""  

        complex_attrs = {}
        for key in kwargs:

            if key not in default_attrs:
                continue # Skip the attribute if it is not a default attribute.

            # 1. Don't validate if the default attribute has the default values.                      
            validate = True
            if kwargs[key] == default_attrs[key]:
                validate = False

            # 2. Skip the complex attributes.
            if hasattr(research_object, "save_" + key):
                complex_attrs[key] = kwargs[key]
                continue # Complex attributes are set in the next step.

            # 3. Skip the attribute if it was previously loaded and the value has not changed (even if it was a kwarg).
            if key in research_object.__dict__ and getattr(research_object, key) == kwargs[key]:
                continue

            # 4. Validate the attribute.
            if validate:
                ResearchObjectHandler.validate(research_object, key, kwargs[key], action)

            if hasattr(research_object, "to_json_" + key):
                to_json_method = getattr(research_object, "to_json_" + key)
                json_value = to_json_method(kwargs[key], action)
            else:
                json_value = json.dumps(kwargs[key])  

            simple_params = (action.id, research_object.id, ResearchObjectHandler._get_attr_id(key), json_value)
            action.add_sql_query(research_object.id, "robj_simple_attr_insert", simple_params, group_name = "robj_simple_attr_insert")

            # 6. Get the parameters for the SQL query to set the attribute.            
            research_object.__dict__[key] = kwargs[key] # Set the attribute in the object's __dict__.        

        # 2. Set complex builtin attributes.
        for key in complex_attrs:

            # 1. Don't validate if the default attribute has the default values.
            # Complex attributes checked for being overwritten in the previous step.                      
            validate = True
            if kwargs[key] == default_attrs[key]:
                validate = False

            # 2. Skip the attribute if it was previously loaded and the value has not changed (even if it was a kwarg).
            if key in research_object.__dict__ and getattr(research_object, key) == complex_attrs[key]:
                continue

            # 3. Validate the attribute.
            if validate:
                ResearchObjectHandler.validate(research_object, key, complex_attrs[key], action)

            # 4.  Save the "complex" builtin attribute to the database.
            save_method = getattr(research_object, "save_" + key)
            save_method(complex_attrs[key], action = action)

            research_object.__dict__[key] = complex_attrs[key]     

    @staticmethod
    def _set_vr_attributes(research_object: "ResearchObject", name: str, value: Any, action: Action) -> None:
        from ResearchOS.variable import Variable 
        
        # Set custom (VR) attributes: this object's "name" attribute is the VR object.
        cursor = action.conn.cursor()
        if name in research_object.__dict__:
            selected_vr = research_object.__dict__[name]                      
        else:            
            # Search through pre-existing unassociated VR for one with this name and is_active = 1. If it's not in the vr_dataobjects table with is_active = 1, it's unassociated.
            # Then, put the vr_id into the vr_dataobjects table.
            # Check if there is an existing variable that should be used (from a different DataObject, e.g. another Trial).            
            selected_vr = None
            sqlquery = "SELECT vr_id FROM vr_dataobjects WHERE dataobject_id = ? AND is_active = ?"
            associated_result = cursor.execute(sqlquery, (research_object.id, 1)).fetchall()
            vr_ids = [row[0] for row in associated_result]
            assoc_vr_ids_unique = list(set(vr_ids))                      
            for vr_id in assoc_vr_ids_unique:
                vr = Variable(id = vr_id)
                if vr.name == name:
                    selected_vr = vr
                    break

            # If no associated VR with that name exists, then select an unassociated VR with that name.
            if not selected_vr: 
                sqlquery = "SELECT object_id FROM research_objects WHERE object_id LIKE ?"
                all_ids = cursor.execute(sqlquery, (Variable.prefix + "%",)).fetchall()
                unassoc_vr_ids = [row[0] for row in all_ids if row[0] not in assoc_vr_ids_unique]                
                if not unassoc_vr_ids:
                    raise ValueError("No unassociated VR with that name exists.")
                # Load each variable and check its name. If no matches here, just take the first one.
                for count, vr_id in enumerate(unassoc_vr_ids):
                    vr = Variable(id = vr_id, action = action)
                    if count == 0:
                        selected_vr = vr
                    if vr.name == name:
                        selected_vr = vr
                        break

            params = (action.id, research_object.id, selected_vr.id)
            action.add_sql_query(research_object.id, "vr_to_dobj_insert", params, group_name = "robj_vr_attr_insert")  
        
        # Check if this value already exists in the database. If so, don't save it again.
        pool_data = SQLiteConnectionPool(name = "data")
        conn_data = pool_data.get_connection()
        cursor_data = conn_data.cursor()
        data_blob = pickle.dumps(value)          
        data_hash = sha256(data_blob).hexdigest()
        params = (data_hash,)
        rows = cursor_data.execute(action.queries["data_blob_id_select"], params).fetchall()
        if rows:
            data_blob_id = rows[0][0]
        else:
            # Save this new value to the data_blob_values table in the data db.                  
            params = (data_hash, data_blob)
            cursor_data.execute(action.queries["data_value_in_blob_insert"], params)
            conn_data.commit()
            data_blob_id = cursor_data.lastrowid
        pool_data.return_connection(conn_data)

        # Put the data_blob_id into the data_values table.
        vr = selected_vr
        ds_id = research_object.get_dataset_id()
        schema_id = vr.get_current_schema_id(ds_id)
        params = (action.id, vr.id, research_object.id, schema_id, data_blob_id)        
        action.add_sql_query(research_object.id, "vr_value_for_dobj_insert", params, group_name = "robj_vr_attr_insert")

        # action.commit = True
        # action.exec = True
        # action.execute(return_conn = False) # Execute the action but keep the connection.
        research_object.__dict__[name] = vr

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
    def _set_simple_builtin_attr(self, name: str, value: Any, action: Action = None) -> None:
        """Set the attributes of a research object in memory and in the SQL database.
        Validates the attribute if it is a built-in ResearchOS attribute (i.e. a method exists to validate it).
        If it is not a built-in ResearchOS attribute, then no validation occurs."""
        # TODO: Have already implemented adding current_XXX_id object to digraph in the database, but should also update the in-memory digraph.     
        
        if hasattr(self, "to_json_" + name):
            to_json_method = getattr(self, "to_json_" + name)
            json_value = to_json_method(value, action)
        else:
            json_value = json.dumps(value)   

        ResearchObjectHandler.save_simple_attribute(self.id, name, json_value, action = action)            
        self.__dict__[name] = value
    
