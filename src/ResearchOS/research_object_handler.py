import weakref
from typing import Any

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