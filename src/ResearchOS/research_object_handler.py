import weakref
from typing import Any
import json

from ResearchOS.db_connection_factory import DBConnectionFactory
from ResearchOS.action import Action

class ResearchObjectHandler:
    """Keep track of all instances of all research objects. This is an static class."""

    instances = weakref.WeakValueDictionary() # Keep track of all instances of all research objects.
    counts = {} # Keep track of the number of instances of each ID.

    @staticmethod
    def check_inputs(tmp_kwargs: dict) -> None:
        """Validate the inputs to the constructor."""
        # Convert the keys of the tmp_kwargs to lowercase.
        kwargs = {}
        for key in tmp_kwargs.keys():
            kwargs[str(key).lower()] = tmp_kwargs[key]
        if not kwargs or "id" not in kwargs.keys():
            raise ValueError("id is required as a kwarg")
        id = kwargs["id"]
        # Ensure that the ID is a string and is properly formatted.

        return kwargs
    
    @staticmethod
    def validator(research_object, attr_name: str, attr_value: Any)  -> Any:
        """Validate the attribute value. If the attribute value is not valid, an error is thrown.
        The "research_object" argument can't currently be type hinted because it would create a circular import."""
        try:            
            validate_method = eval("research_object.validate_" + attr_name)
            validate_method(attr_value)
        except AttributeError as e:
            pass

    @staticmethod
    def from_json(research_object, attr_name: str, attr_value_json: Any) -> Any:
        """Convert the JSON string to an attribute value. If there is no class-specific way to do it, then use the builtin json.loads
        The "research_object" argument can't currently be type hinted because it would create a circular import."""
        try:
            from_json_method = eval("research_object.from_json_" + attr_name)
            attr_value = from_json_method(attr_value_json)
        except AttributeError as e:
            attr_value = json.loads(attr_value_json)
        return attr_value
    
    @staticmethod
    def to_json(research_object, attr_name: str, attr_value: Any) -> Any:        
        """Convert the attribute value to a JSON string. If there is no class-specific way to do it, then use the builtin json.dumps
        The "research_object" argument can't currently be type hinted because it would create a circular import."""
        try:
            to_json_method = eval("research_object.to_json_" + attr_name)
            attr_value_json = to_json_method(attr_value)
        except AttributeError as e:
            attr_value_json = json.dumps(attr_value)
        return attr_value_json
    
    @staticmethod
    def object_exists(id: str) -> bool:
        """Return true if the specified id exists in the database, false if not."""
        db = DBConnectionFactory.create_db_connection()
        cursor = db.conn.cursor()
        sqlquery = f"SELECT object_id FROM research_objects WHERE object_id = '{id}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        return len(rows) > 0
    
    @staticmethod
    def _create_ro(research_object, action: Action) -> None:
        """Create the research object in the research_objects table of the database."""        
        sqlquery = f"INSERT INTO research_objects (object_id, action_id) VALUES ('{research_object.id}', '{action.id}')"
        action.add_sql_query(sqlquery)
        action.execute(commit = False)  

    def _load_ro(research_object, default_attrs: dict) -> None:
        """Load "simple" attributes from the database."""
        # 1. Get the database cursor.
        db = DBConnectionFactory.create_db_connection()
        cursor = db.conn.cursor()

        # 2. Get the attributes from the database.        
        sqlquery = f"SELECT action_id, attr_id, attr_value FROM simple_attributes WHERE object_id = '{research_object.id}'"
        unordered_attr_result = cursor.execute(sqlquery).fetchall()
        ordered_attr_result = ResearchObjectHandler._get_time_ordered_result(unordered_attr_result, action_col_num = 0)
        if len(unordered_attr_result) == 0:
            raise ValueError("No object with that ID exists.")          
                             
        curr_obj_attr_ids = [row[1] for row in ordered_attr_result]
        num_attrs = len(list(set(curr_obj_attr_ids))) # Get the number of unique action ID's.
        used_attr_ids = []
        attrs = {}
        attrs["id"] = research_object.id
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
            if not (attr_name in default_attrs and attr_value == default_attrs[attr_name]):
                ResearchObjectHandler.validator(research_object, attr_name, attr_value)
            attrs[attr_name] = attr_value
            if len(used_attr_ids) == num_attrs:
                break # Every attribute is accounted for.      

        # 3. Set the attributes of the object.
        research_object.__dict__.update(attrs)
    
    @staticmethod
    def save_simple_attribute(id: str, name: str, json_value: Any, action: Action) -> Action:
        """If no store_attr method exists for the object attribute, use this default method."""                                      
        sqlquery = f"INSERT INTO simple_attributes (action_id, object_id, attr_id, attr_value) VALUES ('{action.id}', '{id}', '{ResearchObjectHandler._get_attr_id(name)}', '{json_value}')"                
        action.add_sql_query(sqlquery)
        return action
    
    @staticmethod
    def _get_attr_name(attr_id: int) -> str:
        """Get the name of an attribute given the attribute's ID. If it does not exist, return an error."""
        cursor = DBConnectionFactory.create_db_connection().conn.cursor()
        sqlquery = f"SELECT attr_name FROM attributes_list WHERE attr_id = '{attr_id}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) == 0:
            raise Exception("No attribute with that ID exists.")
        return rows[0][0]  
    
    @staticmethod
    def _get_attr_id(attr_name: str) -> str:
        """Get the ID of the attribute."""
        db = DBConnectionFactory.create_db_connection()
        cursor = db.conn.cursor()
        sqlquery = f"SELECT attr_id FROM attributes_list WHERE attr_name = '{attr_name}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) > 0:
            return rows[0][0]
        else:
            sqlquery = f"INSERT INTO attributes_list (attr_name) VALUES ('{attr_name}')"
            cursor.execute(sqlquery)
            db.conn.commit()
            return cursor.lastrowid
        
    @staticmethod
    def _get_time_ordered_result(result: list, action_col_num: int) -> list[str]:
        """Return the result array from conn.cursor().execute() in reverse chronological order (e.g. latest first)."""
        unordered_action_ids = [row[action_col_num] for row in result] # A list of action ID's in no particular order.
        action_ids_str = ', '.join([f'"{action_id}"' for action_id in unordered_action_ids])
        sqlquery = f"SELECT action_id FROM actions WHERE action_id IN ({action_ids_str}) ORDER BY datetime DESC"
        db = DBConnectionFactory.create_db_connection()
        cursor = db.conn.cursor()
        ordered_action_ids = cursor.execute(sqlquery).fetchall()
        if ordered_action_ids is None or len(ordered_action_ids) == 0:
            raise ValueError("No actions found.")
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
    
