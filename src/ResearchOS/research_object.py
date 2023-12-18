from typing import Any, Type
import re
from abc import abstractmethod
import datetime
import weakref
import traceback

from action import Action
from config import ProdConfig

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
        """Create a new data object. If the object already exists, return the existing object."""
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
        
    def __del__(self) -> None:
        """Delete the object from memory."""
        print("Deleting" + self.id)
        # if self.id not in ResearchObject._instances:
        #     raise ValueError("Object not in instances.")
        ResearchObject._instances_count[self.id] -= 1
        if ResearchObject._instances_count[self.id] == 0:
            del ResearchObject._instances[self.id]
            del ResearchObject._instances_count[self.id]

    def __init__(self, name: str, id: str = None, _stack_limit: int = 2) -> None:
        """"""        
        if not id:
            id = self.id
        action = Action.open(name = "created object " + id)
        try:
            # Create the object in the database.
            cursor = Action.conn.cursor()
            sqlquery = f"INSERT INTO research_objects (object_id) VALUES ('{id}')"
            cursor.execute(sqlquery)
            Action.conn.commit()
        except Exception as e:
            print(e)
        # print(traceback.format_stack(limit = _stack_limit))
        if "name" not in self.__dict__:
            self.name = name
        if "deleted" not in self.__dict__:
            self.deleted = False
        action.close() # Close the action, if possible.

    def __setattr__(self, __name: str, __value: Any) -> None:
        """Set the attributes of a research object in memory and in the SQL database."""
        self.__dict__[__name] = __value

        if __name == "id": 
            raise ValueError("Cannot change the ID of a research object.")

        if __name[0] == "_":
            return # Don't log private attributes.
        
        # Open an action if there is not one open currently. Returns the open action if it is already open.
        action = Action.open(name = "attribute_changed")
        
        table_name = "research_objects"
        cursor = Action.conn.cursor()        
        # Create the object in the database, in the table that contains only the complete list of object ID's.        
        sqlquery = f"INSERT INTO research_object_attributes (action_id, object_id, attr_id, attr_value) VALUES ('{action.id}', '{self.id}', '{ResearchObject._get_attr_id(__name)}', '{__value}')"
        cursor.execute(sqlquery)
        Action.conn.commit()
        action.close() # Close the action, if possible.     

    ###############################################################################################################################
    #################################################### end of dunder methods ####################################################
    ###############################################################################################################################

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
    def _prefix_to_table_name(cls) -> str:
        """Convert a prefix to a table name."""
        prefix = cls.prefix
        if prefix in ["PJ", "AN", "LG", "PG", "PR", "ST", "VW"]:
            return "PipelineObjects"
        elif prefix in ["DS", "SJ", "TR", "PH"]:
            return "DataObjects"
        elif prefix in ["VR"]:
            raise NotImplementedError("Which table do Variables go in?") 
        elif prefix in ["RO"]:
            return "research_objects"
        else:
            raise ValueError("Invalid prefix.")

    @abstractmethod
    def load(id: str, cls: Type, action_id: str = None) -> "ResearchObject":
        """Load the current state of a research object from the database. If an action_id is specified, load the state of the object after that action."""
        # 1. Get the current action if not provided.
        cursor = Action.conn.cursor()
        timestamp = datetime.datetime.utcnow()
        if not action_id:
            action = Action.previous() # With no arguments, gets the "current"/most recent action.
            action_id = action.id
            timestamp = action.timestamp_closed

        # 2. Get the action ID's for this object that were closed before the action_id.
        sqlquery = f"SELECT action_id, attr_id, attr_value, child_of FROM research_object_attributes WHERE object_id = '{id}'"
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
        attrs["id"] = id
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
        
        
        research_object = cls(name = attrs["name"], id = id)
        research_object.__dict__.update(attrs)
        return research_object

    @abstractmethod
    def new(self, name: str, cls: Type) -> "ResearchObject":
        research_object = cls(name = name)
        return research_object

    @abstractmethod
    def _get_attr_id(attr_name: str) -> int:
        """Get the ID of an attribute. If it does not exist, create it."""
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
        """Get the name of an attribute."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT attr_name FROM Attributes WHERE attr_id = '{attr_id}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) == 0:
            raise Exception("No attribute with that ID exists.")
        return rows[0][0]
    
    @abstractmethod
    def is_id(id: str) -> bool:
        """Check if the given ID is a valid ID for this research object."""        
        pattern = "^[a-zA-Z]{2}[a-fA-F0-9]{6}_[a-fA-F0-9]{3}$"
        if not re.match(pattern, id):
            return False
        return True
    
    ###############################################################################################################################
    #################################################### end of abstract methods ##################################################
    ###############################################################################################################################

    def _get_all_parents(self, parent_id: str, child_table_name: str, parent_column: str) -> list[str]:
        """Get all parents of the child object type.
        Dataset > Subject > Visit > Trial > Phase."""
        sql = f'SELECT uuid FROM {child_table_name} WHERE {parent_column} = "{parent_id}"'
        cursor = Action.conn.cursor()
        cursor.execute(sql)
        data = []
        for row in cursor:
            data.append(row['uuid'])
        return data
    
    def _get_parent(self, child_uuid: str, child_table_name: str, parent_column: str) -> str:
        """Get the parent of the child object type.
        Dataset > Subject > Visit > Trial > Phase."""
        sql = f'SELECT {parent_column} FROM {child_table_name} WHERE uuid = "{child_uuid}"'
        cursor = Action.conn.cursor()
        cursor.execute(sql)
        data = []
        for row in cursor:
            data.append(row[parent_column])
        if len(data) > 1:
            raise ValueError(f"Expected <=1 parent, got {len(data)}.")
        if len(data) == 1:
            data = data[0]
        else:
            data = None
        return data
    
    def _is_parent(self, id: str, parent_id: str, table_name: str, parent_column: str, child_column: str) -> bool:
        """Check if the provided parent type is the parent of the child object."""
        action = Action.previous()
        sql = f"SELECT object_id FROM research_object_attributes WHERE child_of = {parent_id} AND object_id = {id} AND action_id = {action.id}"
        cursor = Action.conn.cursor()
        cursor.execute(sql)
        return len(cursor.fetchall()) > 0
    
    def _get_all_children(self, uuid: str, column: str, child_table_name: str) -> list[str]:
        """Get all children of the parent object type.
        Dataset > Subject > Visit > Trial > Phase."""
        sql = f'SELECT uuid FROM {child_table_name} WHERE {column} = "{uuid}"'
        cursor = Action.conn.cursor()
        cursor.execute(sql)
        data = []
        for row in cursor:
            data.append(row['uuid'])
        return data
    
    def _is_child(self, child_id: str) -> bool:
        """Check if the provided child type is the child of the parent (self) object."""
        action = Action.previous()
        sql = f"SELECT object_id FROM research_object_attributes WHERE child_of = {self.id} AND object_id = {child_id} AND action_id = {action.id}"
        cursor = Action.conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        return len(result) > 0
    
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
    from pipeline_objects.project import Project
    # pj1 = Project(name = "Test")
    # pj2 = Project.load(id = pj1.id)
    print(ResearchObject._instances)
    pj = Project.new_current(name = "Test")
    print(ResearchObject._instances)
    print(pj)