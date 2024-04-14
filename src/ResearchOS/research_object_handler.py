import weakref
from typing import Any
import json
from typing import TYPE_CHECKING
import sqlite3
from datetime import datetime, timezone

import numpy as np

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

from ResearchOS.action import Action
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.sql.sql_joiner_most_recent import sql_joiner_most_recent
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.current_user import CurrentUser
from ResearchOS.get_computer_id import COMPUTER_ID
from ResearchOS.validator import Validator
from ResearchOS.json_converter import JSONConverter

do_run = False

class ResearchObjectHandler:
    """Keep track of all instances of all research objects. This is an static class."""

    instances = weakref.WeakValueDictionary() # Keep track of all instances of all research objects.
    counts = {} # Keep track of the number of instances of each ID.    
    # pool = SQLiteConnectionPool(name = "main")
    # pool_data = SQLiteConnectionPool(name = "data")
    default_attrs = {} # Keep track of the default attributes for each class.     

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
            return {} # The research object exists (if it is in the research_objects table), but there are no simple attributes.
        
        if not ordered_attr_result_computer and computer_specific_attr_names:
            raise ValueError("No computer-specific attributes exist for this object.")

        # Computer-specific and computer-independent attributes.
        attrs = {}
        for attr_id, value in ordered_attr_result_dict.items():            
            attr_name = ResearchObjectHandler._get_attr_name(attr_id)
            attr_value = JSONConverter.from_json(research_object, attr_name, value, action)
            attrs[attr_name] = attr_value        

        # 3. Load the class-specific/"complex" builtin attributes.
        for attr_name in default_attrs.keys():
            if hasattr(research_object, "load_" + attr_name):
                load_method = getattr(research_object, "load_" + attr_name)
                value = load_method(action)
                attrs[attr_name] = value

        return attrs

    @staticmethod
    def _set_builtin_attributes(research_object: "ResearchObject", default_attrs: dict, kwargs: dict, action: Action):
        """Responsible for setting the value of all builtin attributes, simple or not."""

        # 1. Validate
        validator = Validator(research_object, action)
        validator.validate(kwargs, default_attrs)

        for key in kwargs:

            # 2. Skip the attribute if the value has not changed.
            if key in research_object.__dict__ and getattr(research_object, key) == kwargs[key]:
                continue

            # 3. Save simple & complex attributes.
            if hasattr(research_object, "save_" + key):
                save_method = getattr(research_object, "save_" + key)
                save_method(kwargs[key], action = action)
            else:            
                json_value = JSONConverter.to_json(research_object, key, kwargs[key], action) 
                simple_params = (action.id_num, research_object.id, ResearchObjectHandler._get_attr_id(key), json_value)
                action.add_sql_query(research_object.id, "robj_simple_attr_insert", simple_params, group_name = "robj_simple_attr_insert")

            # 4. Set the attribute in the object's __dict__.
            research_object.__dict__[key] = kwargs[key] # Set the attribute in the object's __dict__.         
    
    @staticmethod
    def _get_attr_name(attr_id: int) -> str:
        """Get the name of an attribute given the attribute's ID. If it does not exist, return an error."""
        pool = SQLiteConnectionPool()
        conn = pool.get_connection()
        cursor = conn.cursor()
        sqlquery = f"SELECT attr_name FROM attributes_list WHERE attr_id = '{attr_id}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) == 0:
            raise Exception("No attribute with that ID exists.")
        pool.return_connection(conn)
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
        pool = SQLiteConnectionPool()
        conn = pool.get_connection()
        cursor = conn.cursor()
        ordered_action_ids = cursor.execute(sqlquery).fetchall()
        pool.return_connection(conn)
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
        subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
        for cls in subclasses:
            if hasattr(cls, "prefix") and prefix.startswith(cls.prefix):
                return cls
        raise ValueError("No class with that prefix exists.")
    
    # @staticmethod
    # def _set_simple_builtin_attr(research_object, name: str, value: Any, action: Action = None) -> None:
    #     """Set the attributes of a research object in memory and in the SQL database.
    #     Validates the attribute if it is a built-in ResearchOS attribute (i.e. a method exists to validate it).
    #     If it is not a built-in ResearchOS attribute, then no validation occurs."""
    #     # TODO: Have already implemented adding current_XXX_id object to digraph in the database, but should also update the in-memory digraph.     
        
    #     if hasattr(research_object, "to_json_" + name):
    #         to_json_method = getattr(research_object, "to_json_" + name)
    #         json_value = to_json_method(value, action)
    #     else:
    #         json_value = json.dumps(value)   

    #     ResearchObjectHandler.save_simple_attribute(research_object.id, name, json_value, action = action)            
    #     research_object.__dict__[name] = value

    @staticmethod
    def get_user_computer_path(research_object, attr_name: str, action: Action) -> str:
        """Get a user- and computer-specific path, which is a simple attribute in the database."""
        # Load the most recent path.
        attr_id = ResearchObjectHandler._get_attr_id(attr_name)
        sqlquery_raw = "SELECT action_id_num, attr_value FROM simple_attributes WHERE attr_id = ? AND object_id = ?"
        sqlquery = sql_order_result(action, sqlquery_raw, ["attr_id", "object_id"], single = True, user = True, computer = True)
        params = (attr_id, research_object.id)
        result = action.conn.execute(sqlquery, params).fetchall()
        if not result:
            return None
        path = result[0][1]
        path = json.loads(path)

        # Get the timestamp of this action_id
        sqlquery = "SELECT datetime FROM actions WHERE action_id_num = ?"
        params = (result[0][0],)
        timestamp_path = action.conn.execute(sqlquery, params).fetchone()[0]
        
        # Check if the action_id is after the current user/computer action_id.
        sqlquery_raw = "SELECT action_id_num FROM users_computers WHERE user_id = ? AND computer_id = ?"
        # sqlquery = sql_order_result(action, sqlquery_raw, ["user_id", "computer_id"], single = True, user = False, computer = False)
        sqlquery = sql_joiner_most_recent(sqlquery_raw)
        params = (CurrentUser.current_user, COMPUTER_ID)
        action_id_users = action.conn.execute(sqlquery, params).fetchone()[0]

        # Get the timestamp for when the user ID was set.
        sqlquery = "SELECT datetime FROM actions WHERE action_id_num = ?"
        params = (action_id_users,)
        timestamp_users = action.conn.execute(sqlquery, params).fetchone()[0]

        timestamp_path = datetime.strptime(timestamp_path, "%Y-%m-%d %H:%M:%S.%f%z").replace(tzinfo=timezone.utc)
        timestamp_users = datetime.strptime(timestamp_users, "%Y-%m-%d %H:%M:%S.%f%z").replace(tzinfo=timezone.utc)

        if timestamp_path < timestamp_users:
            return None
        return path    
