import weakref
from typing import Any
import json, re, os
from typing import TYPE_CHECKING
import sqlite3

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject
    from ResearchOS.variable import Variable
    # from ResearchOS.DataObjects.data_object import DataObject

# from ResearchOS.default_attrs import DefaultAttrs
# from ResearchOS.db_connection_factory import DBConnectionFactory
from ResearchOS.action import Action
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.idcreator import IDCreator

class ResearchObjectHandler:
    """Keep track of all instances of all research objects. This is an static class."""

    instances = weakref.WeakValueDictionary() # Keep track of all instances of all research objects.
    counts = {} # Keep track of the number of instances of each ID.
    pool = SQLiteConnectionPool()

    @staticmethod
    def load_vr_value(research_object: "ResearchObject", vr: "Variable") -> Any:
        """Load the value of the variable."""        
        dataset_id = research_object.get_dataset_id()        
        schema_id = research_object.get_current_schema_id(dataset_id)
        conn = ResearchObjectHandler.pool.get_connection()
        cursor = conn.cursor()
        sqlquery = f"SELECT scalar_value FROM data_values WHERE vr_id = '{vr.id}' AND dataobject_id = '{research_object.id}' AND schema_id = '{schema_id}'"
        rows = cursor.execute(sqlquery).fetchall()        
        ResearchObjectHandler.pool.return_connection(conn)
        if len(rows) == 0:
            raise ValueError("No value exists for that VR.")
        value = json.loads(rows[0][0])
        if value is None:
            # Get the file path.
            path = vr.get_vr_file_path(vr.id, dataset_id, schema_id)
            if os.path.exists(path):
                with open(path, "r") as f:
                    value = json.load(path)
        return value

    @staticmethod
    def _set_attr_validator(research_object: "ResearchObject", attr_name: str, attr_value: Any, validate: bool = True) -> None:
        """Set the attribute validator for the specified attribute."""
        if not attr_name.isidentifier():
            raise ValueError(f"{attr_name} is not a valid attribute name.") # Offers some protection for having to eval() the name to get custom function names.
        # if attr_name in research_object.__dict__ and research_object.__dict__.get(attr_name, None) == attr_value:
        #     return # No change.
        if attr_name == "id":
            raise ValueError("Cannot change the ID of a research object.")
        if attr_name == "prefix":
            raise ValueError("Cannot change the prefix of a research object.")
        # Validate the value        
        if validate:                                                      
            ResearchObjectHandler.validator(research_object, attr_name, attr_value)

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
    def validator(research_object: "ResearchObject", attr_name: str, attr_value: Any)  -> Any:
        """Validate the attribute value. If the attribute value is not valid, an error is thrown."""
        try:            
            validate_method = eval("research_object.validate_" + attr_name)
            validate_method(attr_value)
        except AttributeError as e:
            pass

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
    def to_json(research_object: "ResearchObject", attr_name: str, attr_value: Any) -> Any:        
        """Convert the attribute value to a JSON string. If there is no class-specific way to do it, then use the builtin json.dumps"""
        try:
            to_json_method = eval("research_object.to_json_" + attr_name)
            attr_value_json = to_json_method(attr_value)
        except AttributeError as e:
            attr_value_json = json.dumps(attr_value)
        return attr_value_json
    
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
                ResearchObjectHandler.validator(research_object, attr_name, attr_value)
            attrs[attr_name] = attr_value
            if len(used_attr_ids) == num_attrs:
                break # Every attribute is accounted for.
        return attrs

    @staticmethod
    def _load_ro(research_object: "ResearchObject", default_attrs: dict) -> None:
        """Load "simple" attributes from the database."""
        # 1. Get the database cursor.
        # db = DBConnectionFactory.create_db_connection()
        conn = ResearchObjectHandler.pool.get_connection()
        cursor = conn.cursor()

        # 2. Get the attributes from the database.        
        sqlquery = f"SELECT action_id, attr_id, attr_value FROM simple_attributes WHERE object_id = '{research_object.id}'"
        unordered_attr_result = cursor.execute(sqlquery).fetchall()
        ResearchObjectHandler.pool.return_connection(conn)
        ordered_attr_result = ResearchObjectHandler._get_time_ordered_result(unordered_attr_result, action_col_num = 0)
        # if len(unordered_attr_result) == 0:
        #     raise ValueError("No object with that ID exists.")          
                             
        attrs = ResearchObjectHandler._get_most_recent_attrs(research_object, ordered_attr_result, default_attrs)   

        if attrs is None:
            raise ValueError("No object with that ID exists.")

        # 3. Set the attributes of the object.
        research_object.__dict__.update(attrs)

        # 4. Load the class-specific attributes.        
        research_object.load()

    @staticmethod
    def _set_builtin_attribute(research_object: "ResearchObject", name: str, value: Any, action: Action, validate: bool, default_attrs: dict, complex_attrs: list[str]):
        """Responsible for setting the value of all builtin attributes, simple or not."""
        # 1. If the attribute name is in default_attrs, it is a builtin attribute, so set the attribute value.
        simple = False

        if name not in complex_attrs:
            simple = True

        ResearchObjectHandler._set_attr_validator(research_object, attr_name=name, attr_value=value, validate=validate) # Validate the attribute.

        if simple:
            # Set the attribute "name" of this object as the VR ID (as a simple attribute).
            ResearchObjectHandler._set_simple_builtin_attr(research_object, name, value, action, validate)
            return        

        # Save the attribute to the database.
        save_method = eval("research_object.save_" + name)
        save_method(value, action = action)

        if action.do_exec:
            action.execute()
        research_object.__dict__[name] = value 

    @staticmethod
    def _setattr(research_object: "ResearchObject", name: str, value: Any, action: Action, validate: bool, default_attrs: dict, complex_attrs: list[str]) -> None:
        """Set the attribute value for the specified attribute. This method serves as ResearchObject.__setattr__()."""
        from ResearchOS.variable import Variable  

        if name in default_attrs:
            ResearchObjectHandler._set_builtin_attribute(research_object, name, value, action, validate, default_attrs, complex_attrs)
            return
        
        conn = action.pool.get_connection()
        cursor = conn.cursor()
        if name in research_object.__dict__:
            # The VR already exists for this name in the research object
            vr = research_object.__dict__[name]
            vr_id = vr.id
        else:            
            # Search through pre-existing unassociated VR for one with this name. If it's not in the vr_dataobjects table, it's unassociated.
            # Then, put the vr_id into the vr_dataobjects table.
            # Check if there is an existing variable that should be used (from a different DataObject, e.g. another Trial).
            selected_vr = None
            sqlquery = f"SELECT vr_id FROM vr_dataobjects"
            associated_result = cursor.execute(sqlquery).fetchall()
            vr_ids = [row[0] for row in associated_result]
            for vr_id in vr_ids:
                vr = Variable(id = vr_id)
                if vr.name == name:
                    selected_vr = vr
                    break

            # If no associated VR with that name exists, then select an unassociated VR with that name.
            if not selected_vr: 
                sqlquery = f"SELECT object_id FROM research_objects"
                all_ids = cursor.execute(sqlquery).fetchall()
                unassoc_vr_ids = [row[0] for row in all_ids if row[0] not in vr_ids and row[0].startswith(Variable.prefix)]
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

        # Put the value into the data_values table.
        vr = selected_vr
        ds_id = research_object.get_dataset_id()
        schema_id = vr.get_current_schema_id(ds_id)
        if ResearchObjectHandler.is_scalar(value):
            json_value = json.dumps(value)
            sqlquery = f"INSERT INTO data_values (action_id, vr_id, dataobject_id, scalar_value, schema_id) VALUES ('{action.id}', '{vr_id}', '{research_object.id}', '{json_value}', '{schema_id}')"
        else:
            sqlquery = f"INSERT INTO data_values (action_id, vr_id, dataobject_id, schema_id) VALUES ('{action.id}', '{vr_id}', '{research_object.id}', '{schema_id}')"
        action.add_sql_query(sqlquery)
        research_object.__dict__[name] = vr
        action.pool.return_connection(conn)
        
    @staticmethod
    def is_scalar(value: Any) -> bool:
        """Return True if the value is a scalar, False if not."""
        if value is None:
            return True
        return isinstance(value, (int, float, str, bool))
    
    @staticmethod
    def save_simple_attribute(id: str, name: str, json_value: Any, action: Action) -> Action:
        """If no store_attr method exists for the object attribute, use this default method."""                                      
        sqlquery = f"INSERT INTO simple_attributes (action_id, object_id, attr_id, attr_value) VALUES ('{action.id}', '{id}', '{ResearchObjectHandler._get_attr_id(name)}', '{json_value}')"                
        action.add_sql_query(sqlquery)
    
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
    def _get_time_ordered_result(result: list, action_col_num: int) -> list[str]:
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
        
        json_value = ResearchObjectHandler.to_json(self, name, value) # Convert the value to JSON
        
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
    
