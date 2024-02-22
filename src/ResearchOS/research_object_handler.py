import weakref
from typing import Any
import json, re, os
from typing import TYPE_CHECKING
import sqlite3, copy
from hashlib import sha256
import pickle

import networkx as nx
import scipy as sp
import numpy as np

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject
    from ResearchOS.DataObjects.data_object import DataObject
    from ResearchOS.variable import Variable

from ResearchOS.idcreator import IDCreator
from ResearchOS.action import Action
from ResearchOS.sqlite_pool import SQLiteConnectionPool

class ResearchObjectHandler:
    """Keep track of all instances of all research objects. This is an static class."""

    instances = weakref.WeakValueDictionary() # Keep track of all instances of all research objects.
    counts = {} # Keep track of the number of instances of each ID.    
    pool = SQLiteConnectionPool(name = "main")
    pool_data = SQLiteConnectionPool(name = "data")    

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
        conn = ResearchObjectHandler.pool_data.get_connection()
        cursor = conn.cursor()
        sqlquery = "SELECT data_blob FROM data_values_blob WHERE data_blob_id = ?"        
        params = (data_blob_id,)
        rows = cursor.execute(sqlquery, params).fetchall()
        ResearchObjectHandler.pool_data.return_connection(conn)
        return pickle.loads(rows[0][0])

    @staticmethod
    def check_inputs(cls: type, tmp_kwargs: dict) -> None:
        """Validate the inputs to the constructor."""        
        # Convert the keys of the tmp_kwargs to lowercase.
        kwargs = {}
        for key in tmp_kwargs.keys():
            kwargs[str(key).lower()] = tmp_kwargs[key]
        if not kwargs or "id" not in kwargs.keys():
            raise ValueError("id is required as a kwarg")
        id = kwargs["id"]
        if not IDCreator().is_ro_id(id):
            raise ValueError("id is not a valid ID.")              

        return kwargs        

    @staticmethod
    def from_json(research_object: "ResearchObject", attr_name: str, attr_value_json: Any) -> Any:
        """Convert the JSON string to an attribute value. If there is no class-specific way to do it, then use the builtin json.loads"""
        try:
            from_json_method = eval("research_object.from_json_" + attr_name)
            attr_value = from_json_method(attr_value_json)
        except AttributeError as e:
            attr_value = json.loads(attr_value_json)
        return attr_value
    
    @staticmethod
    def object_exists(id: str) -> bool:
        """Return true if the specified id exists in the database, false if not."""
        pool = SQLiteConnectionPool()
        conn = pool.get_connection()
        # db = DBConnectionFactory.create_db_connection()
        cursor = conn.cursor()
        sqlquery = f"SELECT object_id FROM research_objects WHERE object_id = '{id}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        pool.return_connection(conn)
        return len(rows) > 0
    
    @staticmethod
    def _create_ro(research_object: "ResearchObject", action: Action) -> None:
        """Create the research object in the research_objects table of the database."""        
        sqlquery = f"INSERT INTO research_objects (object_id, action_id) VALUES ('{research_object.id}', '{action.id}')"
        action.add_sql_query(sqlquery)
        # action.execute(commit = False)

    @staticmethod
    def _get_most_recent_attrs(research_object: "ResearchObject", ordered_attr_result: list, default_attrs: dict = {}) -> dict:
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
                ResearchObjectHandler.validate(research_object, attr_name, attr_value)
            attrs[attr_name] = attr_value
            if len(used_attr_ids) == num_attrs:
                break # Every attribute is accounted for.
        return attrs

    @staticmethod
    def _load_ro(research_object: "ResearchObject", default_attrs: dict) -> None:
        """Load "simple" attributes from the database."""
        # 1. Get the database cursor.
        from ResearchOS.DataObjects.data_object import DataObject
        from ResearchOS.variable import Variable
        conn = ResearchObjectHandler.pool.get_connection()
        cursor = conn.cursor()

        # 2. Get the attributes from the database.        
        sqlquery = f"SELECT action_id, attr_id, attr_value FROM simple_attributes WHERE object_id = '{research_object.id}'"
        unordered_attr_result = cursor.execute(sqlquery).fetchall()
        ResearchObjectHandler.pool.return_connection(conn)
        ordered_attr_result = ResearchObjectHandler._get_time_ordered_result(unordered_attr_result, action_col_num = 0)         
                             
        attrs = ResearchObjectHandler._get_most_recent_attrs(research_object, ordered_attr_result, default_attrs)   

        if attrs is None:
            raise ValueError("No object with that ID exists.")

        # 3. Set the attributes of the object.
        research_object.__dict__.update(attrs)

        # 4. Load the class-specific builtin attributes.
        for key in default_attrs.keys():
            if key in research_object.__dict__:
                continue
            if hasattr(research_object, "load_" + key):
                load_method = getattr(research_object, "load_" + key)
                load_method()

        dobj_subclasses = DataObject.__subclasses__()
        if research_object.__class__ in dobj_subclasses:
            research_object.load_dataobject_vrs()

    @staticmethod
    def validate(research_object: "ResearchObject", name: str, value: Any) -> None:
        """Validate the value of the attribute."""
        if hasattr(research_object, "validate_" + name):
            validate_method = getattr(research_object, "validate_" + name)
            validate_method(value)

    @staticmethod
    def _set_builtin_attribute(research_object: "ResearchObject", name: str, value: Any, action: Action, validate: bool, default_attrs: dict, complex_attrs: list):
        """Responsible for setting the value of all builtin attributes, simple or not."""  

        # Validate the value
        if validate:       
            ResearchObjectHandler.validate(research_object, name, value)

        if not hasattr(research_object, "save_" + name):
            # Set the attribute "name" of this object as the VR ID (as a simple attribute).
            ResearchObjectHandler._set_simple_builtin_attr(research_object, name, value, action, validate)
            return        

        # Save the "complex" builtin attribute to the database.
        save_method = getattr(research_object, "save_" + name)
        save_method(value, action = action)

        if action.do_exec:
            action.execute()
        research_object.__dict__[name] = value 

    @staticmethod
    def _setattr(research_object: "ResearchObject", name: str, value: Any, action: Action, validate: bool, default_attrs: dict, complex_attrs: list) -> None:
        """Set the attribute value for the specified attribute. This method serves as ResearchObject.__setattr__()."""
        from ResearchOS.variable import Variable  

        if name in default_attrs:
            ResearchObjectHandler._set_builtin_attribute(research_object, name, value, action, validate, default_attrs, complex_attrs)
            return
                
        # Set custom (VR) attributes: this object's "name" attribute is the VR object.
        conn = action.pool.get_connection()
        cursor = conn.cursor()
        if name in research_object.__dict__:
            selected_vr = research_object.__dict__[name]                      
        else:            
            # Search through pre-existing unassociated VR for one with this name and is_active = 1. If it's not in the vr_dataobjects table with is_active = 1, it's unassociated.
            # Then, put the vr_id into the vr_dataobjects table.
            # Check if there is an existing variable that should be used (from a different DataObject, e.g. another Trial).            
            selected_vr = None
            sqlquery = f"SELECT vr_id, is_active FROM vr_dataobjects WHERE dataobject_id = '{research_object.id}'"
            associated_result = cursor.execute(sqlquery).fetchall()
            vr_ids = [row[0] for row in associated_result]
            is_active_int = [row[1] for row in associated_result]
            assoc_vr_ids_unique = list(set(vr_ids))
            vr_ids_unique_dict = {vr_id: is_active_int[vr_ids.index(vr_id)] for vr_id in assoc_vr_ids_unique}                        
            for vr_id, is_active in vr_ids_unique_dict.items():
                if is_active != 1:
                    continue
                vr = Variable(id = vr_id)
                if vr.name == name:
                    selected_vr = vr
                    break

            # If no associated VR with that name exists, then select an unassociated VR with that name.
            if not selected_vr: 
                sqlquery = f"SELECT object_id FROM research_objects"
                all_ids = cursor.execute(sqlquery).fetchall()
                unassoc_vr_ids = [row[0] for row in all_ids if row[0] not in assoc_vr_ids_unique and row[0].startswith(Variable.prefix)]                
                if not unassoc_vr_ids:
                    action.pool.return_connection(conn)
                    raise ValueError("No unassociated VR with that name exists.")
                # Load each variable and check its name. If no matches here, just take the first one.
                for count, vr_id in enumerate(unassoc_vr_ids):
                    vr = Variable(id = vr_id)
                    if count == 0:
                        selected_vr = vr
                    if vr.name == name:
                        selected_vr = vr
                        break

            sqlquery = f"INSERT INTO vr_dataobjects (action_id, dataobject_id, vr_id) VALUES ('{action.id}', '{research_object.id}', '{selected_vr.id}')"
            action.add_sql_query(sqlquery)  
        
        # Check if this value already exists in the database. If so, don't save it again.
        pool_data =SQLiteConnectionPool(name = "data")
        conn_data = pool_data.get_connection()
        cursor_data = conn_data.cursor()
        data_blob = pickle.dumps(value)          
        data_hash = sha256(data_blob).hexdigest()
        sqlquery = f"SELECT data_blob_id FROM data_values_blob WHERE data_hash = ?"
        params = (data_hash,)
        rows = cursor_data.execute(sqlquery, params).fetchall()
        if rows:
            data_blob_id = rows[0][0]
        else:
            # Save this new value to the data_blob_values table in the data db.
            sqlquery = "INSERT INTO data_values_blob (data_hash, data_blob) VALUES (?, ?)"                    
            params = (data_hash, data_blob)
            cursor_data.execute(sqlquery, params)
            conn_data.commit()
            data_blob_id = cursor_data.lastrowid
        pool_data.return_connection(conn_data)

        # Put the data_blob_id into the data_values table.
        vr = selected_vr
        ds_id = research_object.get_dataset_id()
        schema_id = vr.get_current_schema_id(ds_id)        
        sqlquery = "INSERT INTO data_values (action_id, vr_id, dataobject_id, schema_id, data_blob_id) VALUES (?, ?, ?, ?, ?)"
        params = (action.id, vr.id, research_object.id, schema_id, data_blob_id)        
        action.add_sql_query(sqlquery, params)
        action.pool.return_connection(conn)

        action.execute()        
        research_object.__dict__[name] = vr

    # @staticmethod
    # def get_vr_file_path(research_object: "ResearchObject", vr_id: str) -> str:
    #     """Get the file path for the VR for this data object."""
    #     from ResearchOS.DataObjects.dataset import Dataset
    #     ds = Dataset(id = research_object.get_dataset_id())
    #     schema = ds.schema
    #     schema_graph = nx.MultiDiGraph(schema)
    #     schema_order = list(nx.topological_sort(schema_graph))
    #     ordered_levels = []
    #     anc_nodes = nx.ancestors(ds.address_graph, research_object)
    #     node_lineage = [research_object] + [anc_node for anc_node in anc_nodes]
    #     for level in schema_order[0:len(node_lineage)]:
    #         ordered_levels.append([n for n in node_lineage if isinstance(n, level)])
    #     data_path = os.sep.join([os.path.dirname(ds.dataset_path) ,"MAT Data Files"])
    #     if not os.path.exists(data_path):            
    #         os.makedirs(data_path)
    #     for level in ordered_levels[1:]:
    #         data_path = os.path.join(data_path, level[0].name)
    #         if not os.path.exists(data_path):         
    #             os.makedirs(data_path)
    #     file_path = data_path + os.sep + vr_id + ".mat"
    #     return file_path

    # @staticmethod
    # def save_vr_to_file(research_object: "ResearchObject", ro_id: str, vr_id: str, schema_id: str, raw_value: Any) -> None:
    #     file_path = ResearchObjectHandler.get_vr_file_path(research_object, vr_id)        
    #     # Save the data to the file.
    #     tmp_value = copy.deepcopy(raw_value)
    #     value = ResearchObjectHandler.clean_value_for_save_mat(tmp_value)        
    #     # sp.io.savemat(file_path, {"stored_data": {"key1": [1, 2, 3], "key2": [4, 5, 6], "key3": [1, "b", "cd"]}})
    #     sp.io.savemat(file_path, {"stored_data": value})

    # @staticmethod
    # def load_vr_from_file(research_object: "ResearchObject", ro_id: str, vr_id: str, schema_id: str) -> Any:
    #     """Load the value of this variable from the file."""
    #     file_path = ResearchObjectHandler.get_vr_file_path(research_object, vr_id)
    #     if not os.path.exists(file_path):
    #         return

    #     # Load the data from the file.
    #     scipy_val = sp.io.loadmat(file_path)
    #     data_out = scipy_val["stored_data"]        
    #     cleaned_data = ResearchObjectHandler.clean_value_from_load_mat(data_out)
    #     return cleaned_data



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
        
    # @staticmethod
    # def is_scalar(value: Any) -> bool:
    #     """Return True if the value is a scalar, False if not."""
    #     if value is None:
    #         return True
    #     return isinstance(value, (int, float, str, bool))
    
    @staticmethod
    def save_simple_attribute(id: str, name: str, json_value: Any, action: Action) -> Action:
        """If no store_attr method exists for the object attribute, use this default method."""                                      
        sqlquery = f"INSERT INTO simple_attributes (action_id, object_id, attr_id, attr_value) VALUES (?, ?, ?, ?)"
        params = (action.id, id, ResearchObjectHandler._get_attr_id(name), json_value)
        action.add_sql_query(sqlquery, params)
    
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
    def _set_simple_builtin_attr(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attributes of a research object in memory and in the SQL database.
        Validates the attribute if it is a built-in ResearchOS attribute (i.e. a method exists to validate it).
        If it is not a built-in ResearchOS attribute, then no validation occurs."""
        # TODO: Have already implemented adding current_XXX_id object to digraph in the database, but should also update the in-memory digraph.     
        
        if hasattr(self, "to_json_" + name):
            to_json_method = getattr(self, "to_json_" + name)
            json_value = to_json_method(value)
        else:
            json_value = json.dumps(value)
        
        # Create an Action. Must be before the ResearchObjectHandler.save_simple_attribute() method.
        execute_action = False
        if action is None:
            execute_action = True
            action = Action(name = "attribute_changed")      

        ResearchObjectHandler.save_simple_attribute(self.id, name, json_value, action = action)            
        # If the attribute contains the words "current" and "id" and the ID has been validated, add a digraph edge between the two objects with an attribute.
        pattern = r"^current_[\w\d]+_id$"
        if re.match(pattern, name):
            pass
            # action = self._default_store_edge_attr(target_object_id = value, name = name, value = DEFAULT_EXISTS_ATTRIBUTE_VALUE, action = action)
            # if self.__dict__.get(name, None) != value:
            #     execute_action = True # Need to execute an action if adding an edge.
        if execute_action:
            action.execute()
        self.__dict__[name] = value
    
