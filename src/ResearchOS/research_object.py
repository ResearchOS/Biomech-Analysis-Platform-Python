from typing import Any, Type
import re
from abc import abstractmethod
import datetime
import weakref
import traceback

from src.ResearchOS.action import Action
from src.ResearchOS.config import ProdConfig

abstract_id_len = ProdConfig.abstract_id_len
instance_id_len = ProdConfig.instance_id_len

class ResearchObject():
    """One research object. Parent class of Data Objects & Pipeline Objects."""

    prefix = "RO" # Testing only
    _instances = weakref.WeakValueDictionary()
    _instances_count = {}
    
    def __hash__(self):
        return hash(self.id)
    
    def __eq__(self, other):
        if isinstance(other, ResearchObject):
            return self.id == other.id
        return NotImplemented

    def __new__(cls, *args, **kwargs):
        """Create a new research object. If the object already exists, return the existing object."""
        object_id = kwargs.get("id", None)
        if object_id is None:
            object_id = cls.create_id(cls)
        if object_id in ResearchObject._instances:
            ResearchObject._instances_count[object_id] += 1
            return ResearchObject._instances[object_id]
        else: # Create a new object.
            instance = super(ResearchObject, cls).__new__(cls)
            ResearchObject._instances[object_id] = instance
            ResearchObject._instances_count[object_id] = 1
            instance.__dict__['id'] = object_id            
            return instance

    def __init__(self, name: str = "object creation", id: str = None, _stack_limit: int = 2) -> None:
        """"""        
        action = Action(name = name)
        if not id:
            id = self.id # self.id always exists by this point thanks to __new__      
        try:
            # Create the object in the database.            
            sqlquery = f"INSERT INTO research_objects (object_id) VALUES ('{id}')"
            action.add_sql_query(sqlquery)
            action.execute()
        except Exception as e:
            print(e)
        # print(traceback.format_stack(limit = _stack_limit))
        if "name" not in self.__dict__:
            self.name = name
        if "deleted" not in self.__dict__:
            self.deleted = False

    def __setattr__(self, __name: str, __value: Any) -> None:
        """Set the attributes of a research object in memory and in the SQL database."""
        self.__dict__[__name] = __value

        if __name == "id": 
            raise ValueError("Cannot change the ID of a research object.")

        if __name[0] == "_":
            return # Don't log private attributes.
        
        # Open an action if there is not one open currently. Returns the open action if it is already open.
        action = Action(name = "attribute_changed")
                       
        # Create the object in the database, in the table that contains only the complete list of object ID's.        
        sqlquery = f"INSERT INTO research_object_attributes (action_id, object_id, attr_id, attr_value) VALUES ('{action.id}', '{self.id}', '{ResearchObject._get_attr_id(__name)}', '{__value}')"
        action.add_sql_query(sqlquery)
        action.execute()  

    def __getattr__(self, __name: str) -> Any:
        """Get the attribute of a research object from the SQL database."""
        if __name[0] == "_":
            raise AttributeError(f"Attribute {__name} does not exist.")
        # Get the attribute from the database.
        sqlquery = f"SELECT attr_value FROM research_object_attributes WHERE object_id = '{self.id}' AND attr_id = '{ResearchObject._get_attr_id(__name)}'"
        cursor = Action.conn.cursor()
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) == 0:
            raise AttributeError(f"Attribute {__name} does not exist.")
        return rows[0][0]

    ###############################################################################################################################
    #################################################### end of dunder methods ####################################################
    ###############################################################################################################################

    @abstractmethod
    def get_all_ids(cls) -> list[str]:
        """Get all object IDs of the specified class."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT object_id FROM research_objects"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        return [row[0] for row in rows if (row[0] is not None and row[0].startswith(cls.prefix))]

    @abstractmethod
    def create_id(cls, abstract: str = None, instance: str = None) -> str:
        """Create a unique ID for the research object."""
        import random
        table_name = "research_objects"
        is_unique = False
        while not is_unique:
            if not abstract:
                abstract_new = str(hex(random.randrange(0, 16**abstract_id_len))[2:]).upper()
                abstract_new = "0" * (abstract_id_len-len(abstract_new)) + abstract_new
            else:
                abstract_new = abstract

            if not instance:
                instance_new = str(hex(random.randrange(0, 16**instance_id_len))[2:]).upper()
                instance_new = "0" * (instance_id_len-len(instance_new)) + instance_new
            else:
                instance_new = instance
 
            id = cls.prefix + abstract_new + "_" + instance_new
            cursor = Action.conn.cursor()
            sql = f'SELECT object_id FROM {table_name} WHERE object_id = "{id}"'
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                is_unique = True
        return id  

    @abstractmethod
    def load(object_id: str, cls: Type, action_id: str = None) -> "ResearchObject":
        """Load the current state of a research object from the database. If an action_id is specified, load the state of the object after that action."""
        # 1. Get the current action if not provided.
        cursor = Action.conn.cursor()
        timestamp = datetime.datetime.utcnow()
        if not action_id:
            action = Action.previous() # With no arguments, gets the "current"/most recent action.
            action_id = action.id
            timestamp = action.timestamp

        # 2. Get the action ID's for this object that were closed before the action_id.
        sqlquery = f"SELECT action_id, attr_id, attr_value, child_of FROM research_object_attributes WHERE object_id = '{object_id}'"
        attr_result = cursor.execute(sqlquery).fetchall()
        if len(attr_result) == 0:
            raise ValueError("No object with that ID exists.")
        curr_obj_action_ids = [row[0] for row in attr_result]
        curr_obj_attr_ids = [row[1] for row in attr_result]
        attrs = {}
        # Get the action ID's for this object by timestamp, descending.        
        curr_obj_action_ids_str = ",".join([f"'{action_id}'" for action_id in curr_obj_action_ids])
        sqlquery = f"SELECT action_id, timestamp_closed FROM actions WHERE action_id IN ({curr_obj_action_ids_str}) ORDER BY timestamp_closed DESC"
        action_ids_in_time_order = cursor.execute(sqlquery).fetchall()
        action_ids_in_time_order = [row[0] for row in action_ids_in_time_order]        
        used_attr_ids = []
        num_attrs = len(list(set(curr_obj_attr_ids))) # Get the number of unique action ID's.
        attrs["id"] = object_id
        attrs["child_of"] = None
        for index, curr_obj_action_id in enumerate(action_ids_in_time_order):            
            attr_id = attr_result[index][1]
            if attr_id in used_attr_ids:
                continue
            used_attr_ids.append(attr_id)            
            attr_value = attr_result[index][2]
            child_of = attr_result[index][3]

            attr_name = ResearchObject._get_attr_name(attr_id)
            attrs[attr_name] = attr_value
            if len(used_attr_ids) == num_attrs:
                break
        
        research_object = cls(name = attrs["name"], id = object_id)
        research_object.__dict__.update(attrs)
        return research_object

    @abstractmethod
    def _get_attr_id(attr_name: str) -> int:
        """Get the ID of an attribute given its name. If it does not exist, create it."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT attr_id FROM Attributes WHERE attr_name = '{attr_name}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) == 0:
            sqlquery = f"INSERT INTO Attributes (attr_name) VALUES ('{attr_name}')"
            cursor.execute(sqlquery)
            sqlquery = f"SELECT attr_id FROM Attributes WHERE attr_name = '{attr_name}'"
            cursor.execute(sqlquery)            
            rows = cursor.fetchall()
            if len(rows) > 1:
                raise Exception("More than one attribute with the same name.")
            
        return rows[0][0]   
    
    @abstractmethod
    def _get_attr_name(attr_id: int) -> str:
        """Get the name of an attribute given the attribute's ID. If it does not exist, return an error."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT attr_name FROM Attributes WHERE attr_id = '{attr_id}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) == 0:
            raise Exception("No attribute with that ID exists.")
        return rows[0][0]    
    
    ###############################################################################################################################
    #################################################### end of abstract methods ##################################################
    ###############################################################################################################################

    def validate_id_class(self, id: str, cls: str) -> None:
        """Validate that the specified ID is a valid ID for the specified class, or None."""
        if id is None:
            return
        if not self.is_id(id):
            raise ValueError(f"Invalid ID.")
        # Check that the ID is of the proper class.        
        id_info  = self.parse_id(id)
        if id_info[0] != cls.prefix:
            raise ValueError(f"ID is not of the proper class.")
        
    def is_id(id: str) -> bool:
        """Check if the given ID matches the pattern of a valid research object ID."""              
        pattern = "^[a-zA-Z]{2}[a-fA-F0-9]{6}_[a-fA-F0-9]{3}$"
        if not re.match(pattern, id):
            return False
        return True    

    def _is_id_or_none(self, id: str) -> bool:
        """Check if the given ID matches the pattern of a valid research object ID, or is None."""              
        if id is None or ResearchObject.is_id(id):
            return True
        return False

    def _get_all_source_object_ids(self, cls) -> list[str]:
        """Get all source object ids of the specified target object of the specified type."""
        sql = f'SELECT object_id FROM research_object_attributes WHERE target_object_id = "{self.id}"'
        self.__get_all_related_object_ids(cls, sql)
    
    def _get_all_target_object_ids(self, cls) -> list[str]:
        """Get all target object ids of the specified source object of the specified type. """
        sql = f'SELECT target_object_id FROM research_object_attributes WHERE object_id = "{self.id}"'
        self.__get_all_related_object_ids(cls, sql)

    def __get_all_related_object_ids(self, cls, sql) -> list[str]:
        """Called by _get_all_source_object_ids and _get_all_target_object_ids.
        Get all related object ids of the specified object of the specified type, either source of target objects."""
        cursor = Action.conn.cursor()
        cursor.execute(sql)
        data = []
        for row in cursor:
            if row[0] is None:
                continue
            if row[0].startswith(cls.prefix):                
                data.append(row[0])
        return data
    
    def _is_source(self, id: str) -> bool:
        """Check if the specified object ID is a source object of the current object."""        
        sql = f"SELECT object_id FROM research_object_attributes WHERE target_object_id = {self.id} AND object_id = {id}"
        cursor = Action.conn.cursor()
        cursor.execute(sql)
        return len(cursor.fetchall()) > 0
    
    def _is_target(self, id: str) -> bool:
        """Check if the specified object ID is a target object of the current object."""        
        sql = f"SELECT target_object_id FROM research_object_attributes WHERE object_id = {self.id} AND target_object_id = {id}"
        cursor = Action.conn.cursor()
        cursor.execute(sql)
        return len(cursor.fetchall()) > 0
    
    def _add_target_object_id(self, id: str, cls: type) -> None:
        """Add a target object ID to the current source object."""
        if not self._is_id(id):
            raise ValueError("Invalid ID.")      
        self.validate_id_class(id, cls)  
        if self._is_target(id):
            return # Already exists.
        sql = f"INSERT INTO research_object_attributes (object_id, target_object_id) VALUES ('{self.id}', '{id}')"
        action = Action(name = "add_target_object_id")
        action.add_sql_query(sql)
        action.execute()

    def _remove_target_object_id(self, id: str, cls: type) -> None:
        """Remove a target object ID from the current source object."""
        if not self._is_id(id):
            raise ValueError("Invalid ID.")      
        self.validate_id_class(id, cls)  
        if not self._is_target(id):
            return
        sql = f"INSERT INTO research_object_attributes (object_id, target_object_id) VALUES ('{self.id}', {None})"
        action = Action(name = "remove_target_object_id")
        action.add_sql_query(sql)
        action.execute()

    def _add_source_object_id(self, id: str, cls: type) -> None:
        """Add a source object ID to the current target object."""
        if not self._is_id(id):
            raise ValueError("Invalid ID.")      
        self.validate_id_class(id, cls)  
        if self._is_source(id):
            return # Already exists
        sql = f"INSERT INTO research_object_attributes (object_id, target_object_id) VALUES ('{id}', '{self.id}')"
        action = Action(name = "add_source_object_id")
        action.add_sql_query(sql)
        action.execute()

    def _remove_source_object_id(self, id: str, cls: type) -> None:
        """Remove a source object ID from the current target object."""
        if not self._is_id(id):
            raise ValueError("Invalid ID.")      
        self.validate_id_class(id, cls)  
        if not self._is_source(id):
            return
        sql = f"INSERT INTO research_object_attributes (object_id, target_object_id) VALUES ('{id}', {None})"
        action = Action(name = "remove_source_object_id")
        action.add_sql_query(sql)
        action.execute()
    
    ###############################################################################################################################
    #################################################### end of parentage methods #################################################
    ############################################################################################################################### 
    
    def parse_id(self, id: str) -> tuple:
        """Parse an ID into its prefix, abstract, and instance parts."""
        if not ResearchObject.is_id(id):
            raise ValueError("Invalid ID.")
        abstract = id[2:2+abstract_id_len]
        instance = id[-instance_id_len:]
        return (self.prefix, abstract, instance)
    
    def _get_public_keys(self) -> list[str]:
        """Return all public keys of the current object."""        
        keys = []
        for key in vars(self).keys():
            if not key.startswith('_'):
                keys.append(key)
        return keys
    
if __name__=="__main__":
    # Cannot run anything from here because Action is a circular import.
    pass