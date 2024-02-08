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
    def validator(attr_name: str, attr_value: Any)  -> Any:
        """Validate the attribute value. If the attribute value is not valid, an error is thrown."""
        try:            
            validate_method = eval("self.validate_" + attr_name)
            validate_method(attr_value)
        except AttributeError as e:
            pass

    @staticmethod
    def from_json(attr_name: str, attr_value_json: Any) -> Any:
        try:
            from_json_method = eval("self.from_json_" + attr_name)
            attr_value = from_json_method(attr_value_json)
        except AttributeError as e:
            attr_value = json.loads(attr_value_json)
        return attr_value
    
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
    def save_simple_attribute(id: str, name: str, value: Any, json_value: Any, action: Action) -> Action:
        """If no store_attr method exists for the object attribute, use this default method."""                                      
        sqlquery = f"INSERT INTO simple_attributes (action_id, object_id, attr_id, attr_value) VALUES ('{action.id}', '{id}', '{ResearchObjectHandler._get_attr_id(name, value)}', '{json_value}')"                
        action.add_sql_query(sqlquery)
        return action
    
    @staticmethod
    def _get_attr_id(name: str, value: Any) -> str:
        """Get the ID of the attribute."""
        db = DBConnectionFactory.create_db_connection()
        cursor = db.conn.cursor()
        sqlquery = f"SELECT attr_id FROM attributes_list WHERE attr_name = '{name}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) > 0:
            return rows[0][0]
        else:
            sqlquery = f"INSERT INTO attributes_list (attr_name) VALUES ('{name}')"
            cursor.execute(sqlquery)
            db.conn.commit()
            return cursor.lastrowid
        
    @staticmethod
    def _get_time_ordered_result(result: list, action_col_num: int) -> list[str]:
        """Return the result array from conn.cursor().execute() in reverse chronological order (e.g. latest first)."""
        unordered_action_ids = [row[action_col_num] for row in result] # A list of action ID's in no particular order.
        action_ids_str = ', '.join([f'"{action_id}"' for action_id in unordered_action_ids])
        sqlquery = f"SELECT action_id FROM actions WHERE action_id IN ({action_ids_str}) ORDER BY timestamp DESC"
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