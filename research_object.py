from typing import Any, Type
import re
from abc import abstractmethod

from action import Action



import weakref

abstract_id_len = 6
instance_id_len = 3


class ResearchObject():
    """One research object. Parent class of Data Objects & Pipeline Objects."""

    prefix = "RO" # Testing only
    _instances = weakref.WeakValueDictionary()
    _instances_count = {}

    def __new__(cls, *args, **kwargs):
        """Create a new data object. If the object already exists, return the existing object."""
        object_id = kwargs.get("id", None)
        if object_id in ResearchObject._instances:
            ResearchObject._instances_count[object_id] += 1
            return ResearchObject._instances[object_id]
        else:
            instance = super(ResearchObject, cls).__new__(cls)
            ResearchObject._instances[object_id] = instance
            ResearchObject._instances_count[object_id] = 1
            instance.id = object_id
            return instance
        
    def __del__(self) -> None:
        """Delete the object from memory."""
        if self.id not in ResearchObject._instances:
            raise ValueError("Object not in instances.")
        ResearchObject._instances_count[self.id] -= 1
        if ResearchObject._instances_count[self.id] == 0:
            del ResearchObject._instances[self.id]
            del ResearchObject._instances_count[self.id]

    def __init__(self, name: str, id: str = None) -> None:
        if not id:
            id = self.create_id()
            self.id = id
        self.name = name
        self.deleted = False

    def __setattr__(self, __name: str, __value: Any) -> None:
        """Set the attributes of a research object in memory and in the SQL database."""
        self.__dict__[__name] = __value

        if __name[0] == "_":
            return # Don't log private attributes.
        
        # Open an action if there is not one open currently. Returns the open action if it is already open.
        action = Action.open(name = "Test")
        
        table_name = "research_objects"
        cursor = Action.conn.cursor()        
        # Create the object in the database, in the table that contains only the complete list of object ID's.        
        if __name == "id":                        
            sqlquery = f"INSERT INTO {table_name} (object_id) VALUES ('{self.id}')"            
            cursor.execute(sqlquery)
        else:
            sqlquery = f"INSERT INTO research_object_transactions (action_id, object_id, attr_id, attr_value) VALUES ('{action.id}', '{self.id}', '{ResearchObject._get_attr_id(__name)}', '{__value}')"
            cursor.execute(sqlquery)
        Action.conn.commit()   
        action.close() # Close the action, if possible.     

    ###############################################################################################################################
    #################################################### end of dunder methods ####################################################
    ###############################################################################################################################

    @abstractmethod
    def _get_attr_id(self, attr_name: str) -> int:
        """Get the ID of an attribute. If it does not exist, create it."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT attr_id FROM Attributes WHERE name = '{attr_name}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) == 0:
            sqlquery = f"INSERT INTO Attributes (name) VALUES ('{attr_name}')"
            cursor.execute(sqlquery)
            sqlquery = f"SELECT attr_id FROM Attributes WHERE name = '{attr_name}'"
            cursor.execute(sqlquery)            
            rows = cursor.fetchall()
            if len(rows) > 1:
                raise Exception("More than one attribute with the same name.")
            
        return rows[0][0]   
    
    @abstractmethod
    def _get_attr_name(self, attr_id: int) -> str:
        """Get the name of an attribute."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT name FROM Attributes WHERE attr_id = '{attr_id}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) == 0:
            raise Exception("No attribute with that ID exists.")
        return rows[0][0]
    
    ###############################################################################################################################
    #################################################### end of abstract methods ##################################################
    ###############################################################################################################################

    def _prefix_to_table_name(self) -> str:
        """Convert a prefix to a table name."""
        prefix = self.prefix
        if prefix in ["PJ", "AN", "LG", "PG", "PR", "ST", "VW"]:
            return "PipelineObjects"
        elif prefix in ["DS", "SJ", "TR", "PH"]:
            return "DataObjects"
        elif prefix in ["VR"]:
            raise NotImplementedError("Which table do Variables go in?")

    def create_id(self, abstract: str = None, instance: str = None) -> str:
        """Create a unique ID for the research object."""
        import random
        table_name = self._prefix_to_table_name()
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
 
            id = self.prefix + abstract_new + "_" + instance_new
            cursor = Action.conn.cursor()
            sql = f'SELECT id FROM {table_name} WHERE id = "{id}"'
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                is_unique = True
        return id
    
    @abstractmethod
    def load(id: str, cls: Type, action_id: str = None) -> "ResearchObject":
        """Load the current state of a research object from the database. If an action_id is specified, load the state of the object after that action."""
        cursor = Action.conn.cursor()
        if not action_id:
            action = Action.previous() # With no arguments, gets the "current"/most recent action.
            action_id = action.id

        sqlquery = f"SELECT action_id, attr_id, attr_value, child_of FROM research_object_attributes WHERE object_id = '{id}'"
        result = cursor.execute(sqlquery).fetchall()
        if len(result) == 0:
            raise ValueError("No object with that ID exists.")
        curr_obj_action_ids = [row[0] for row in result]    
        attrs = {}
        # Order the action ID's for this object by timestamp, descending.        
        sqlquery = "SELECT action_id, timestamp_closed FROM actions WHERE action_id IN ({}) ORDER BY timestamp_closed DESC".format(','.join(['%s' for _ in result]))
        action_ids_in_time_order = cursor.execute(sqlquery).fetchall()
        # Put the curr_obj_action_ids in time order, descending.
        curr_obj_action_ids_sorted = [obj for obj in action_ids_in_time_order if obj in curr_obj_action_ids]
        used_attr_ids = []
        for curr_obj_action_id in curr_obj_action_ids_sorted:
            index = curr_obj_action_ids.index(curr_obj_action_id)
            attr_id = result[index][1]
            if attr_id in used_attr_ids:
                continue
            used_attr_ids.append(attr_id)
            attr_value = result[index][2]
            child_of = result[index][3]
            attr_name = ResearchObject._get_attr_name(attr_id)
            attrs[attr_name] = attr_value
        
        attrs["id"] = id
        research_object = cls(name = attrs["name"])
        research_object.__dict__.update(attrs)
        return research_object

    @abstractmethod
    def new(self, name: str, cls: Type) -> "ResearchObject":
        research_object = cls(name = name)
        return research_object
    
    def is_id(self, id: str) -> bool:
        """Check if the given ID is a valid ID for this research object."""        
        pattern = "^[a-zA-Z0-9]{10}_[a-zA-Z0-9]{3}$"
        if not re.match(pattern, id):
            return False
        return True
    
    def parse_id(self, id: str) -> tuple:
        """Parse an ID into its prefix, abstract, and instance parts."""
        if not self.is_id(id):
            raise ValueError("Invalid ID.")
        abstract = id[2:2+abstract_id_len]
        instance = id[-instance_id_len:]
        return (self.prefix, abstract, instance)
    
    def save(self):
        """Save the research object to the database."""
        raise NotImplementedError("Save not implemented.")
    
    def _get_public_keys(self) -> list[str]:
        """Return all public keys of the current object."""        
        keys = []
        for key in vars(self).keys():
            if not key.startswith('_'):
                keys.append(key)
        return keys


    
if __name__=="__main__":
    # ro = ResearchObject(name = "Test")
    # ro.a = 1

    from pipeline_objects.project import Project
    pj = Project.load(id = 1)
    pj = Project.new_current(name = "Test")
    print(pj)