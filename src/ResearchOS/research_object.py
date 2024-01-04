from typing import Any
import re
from abc import abstractmethod
import datetime
import weakref
import json

from src.ResearchOS.action import Action
from src.ResearchOS.config import ProdConfig

abstract_id_len = ProdConfig.abstract_id_len
instance_id_len = ProdConfig.instance_id_len

DEFAULT_EXISTS_ATTRIBUTE_NAME = "exists"
DEFAULT_EXISTS_ATTRIBUTE_VALUE = True
DEFAULT_NAME_ATTRIBUTE_NAME = "name"
DEFAULT_NAME_ATTRIBUTE_VALUE = "object creation" 

class ResearchObject():
    """One research object. Parent class of Data Objects & Pipeline Objects."""

    prefix = "RO" # Testing only
    _objects = weakref.WeakValueDictionary()
    _objects_count = {}   
    
    def __hash__(self):
        return hash(self.id)
    
    def __eq__(self, other):
        if isinstance(other, ResearchObject):
            return self.id == other.id
        return NotImplemented
    
    def __str__(self, class_attr_names: list[str], attrs: dict) -> str:
        #         return_str = "current_analysis_id: " + self.current_analysis_id + "\n" + ...
        # "current_dataset_id: " + self.current_dataset_id + "\n" + ...
        # "project_path: " + self.project_path
        pass

    def __new__(cls, is_abstract: bool = False, *args, **kwargs):
        """Create a new research object. If the object already exists, return the existing object.
        If is_abstract is True, returns an abstract object that does not have an instance ID.
        Otherwise, returns an instance object that has an instance ID."""
        object_id = kwargs.get("id", None)        
        if object_id is None:
            object_id = cls.create_id(cls, is_abstract = is_abstract)
        if object_id in ResearchObject._objects:
            ResearchObject._objects_count[object_id] += 1
            return ResearchObject._objects[object_id]
        else: # Create a new object.
            instance = super(ResearchObject, cls).__new__(cls)
            ResearchObject._objects[object_id] = instance
            ResearchObject._objects_count[object_id] = 1
            instance.__dict__['id'] = object_id
            return instance

    def __init__(self, name: str = DEFAULT_NAME_ATTRIBUTE_NAME, attrs: dict = {}, id: str = None) -> None:
        """id is required but will actually not be used here because it is assigned during __new__"""
        id = self.id # self.id always exists by this point thanks to __new__        
        # Try to load the object from the database.
        try:
            self.load()            
        except ValueError: # Throws an exception if the object does not exist. In that case, create it.            
            action = Action(name = name)        
            try:
                # Create the object in the database.            
                sqlquery = f"INSERT INTO research_objects (object_id) VALUES ('{id}')"
                action.add_sql_query(sqlquery)
                action.execute()
            except Exception as e:
                print(e)   
            if DEFAULT_EXISTS_ATTRIBUTE_NAME not in self.__dict__:
                self.__setattr__(DEFAULT_EXISTS_ATTRIBUTE_NAME, DEFAULT_EXISTS_ATTRIBUTE_VALUE)
            if DEFAULT_NAME_ATTRIBUTE_NAME not in self.__dict__:
                self.__setattr__(DEFAULT_NAME_ATTRIBUTE_NAME, name)
        # Ensure that all of the required attributes are present.
        for attr in attrs:
            if attr in self.__dict__:
                continue
            # Don't validate during object initialization because the initial values won't pass the validation.
            self.__setattr__(attr, attrs[attr], validate = False)            

    def load(self) -> "ResearchObject":
        """Load the current state of a research object from the database. Modifies the self object."""        
        cursor = Action.conn.cursor()

        # 2. Get the action ID's for this object that were closed before the action_id.
        sqlquery = f"SELECT action_id, attr_id, attr_value, target_object_id FROM research_object_attributes WHERE object_id = '{self.id}'"
        attr_result = cursor.execute(sqlquery).fetchall()
        if len(attr_result) == 0:
            raise ValueError("No object with that ID exists.")
        curr_obj_action_ids = [row[0] for row in attr_result]
        curr_obj_attr_ids = [row[1] for row in attr_result]
        attrs = {}
        # Get the action ID's for this object by timestamp, descending.        
        curr_obj_action_ids_str = ",".join([f"'{action_id}'" for action_id in curr_obj_action_ids])
        sqlquery = f"SELECT action_id, timestamp FROM actions WHERE action_id IN ({curr_obj_action_ids_str}) ORDER BY timestamp DESC"
        action_ids_in_time_order = cursor.execute(sqlquery).fetchall()
        action_ids_in_time_order = [row[0] for row in action_ids_in_time_order]        
        used_attr_ids = []
        num_attrs = len(list(set(curr_obj_attr_ids))) # Get the number of unique action ID's.
        attrs["id"] = self.id
        # attrs["source_object_ids"] = [] # Do objects contain a reference to the source object ID's?
        attrs["target_object_ids"] = []
        for curr_obj_action_id in action_ids_in_time_order:
            index = curr_obj_action_ids.index(curr_obj_action_id)
            attr_id = attr_result[index][1]
            if attr_id in used_attr_ids:
                continue
            used_attr_ids.append(attr_id)
            attr_value = json.loads(attr_result[index][2])
            target_object_id = attr_result[index][3]
            if target_object_id not in attrs["target_object_id"] and target_object_id is not None:
                attrs["target_object_id"].append(target_object_id)

            attr_name = ResearchObject._get_attr_name(attr_id)
            attr_type = ResearchObject._get_attr_type(attr_id)
            attrs[attr_name] = attr_type(attr_value)
            eval_str = "self.json_translate_" + attr_name + "()"
            try:
                attrs[attr_name] = eval(eval_str)
            except AttributeError:
                pass
            if len(used_attr_ids) == num_attrs:
                break
                
        self.__dict__.update(attrs)

    def __setattr__(self, __name: str, __value: Any, validate: bool = True) -> None:
        """Set the attributes of a research object in memory and in the SQL database.
        Validates the attribute if it is a built-in ResearchOS attribute (i.e. a method exists to validate it), and the object is not being initialized."""        
        # TODO: Does this get called when deleting an attribute from an object?
        # TODO: Have already implemented adding current_XXX_id object to digraph in the database, but should also update the in-memory digraph.
        if __name == "id":
            raise ValueError("Cannot change the ID of a research object.")
        if __name[0] == "_":
            return # Don't log private attributes.        
        if validate:                                                      
            try:
                method = eval(f"self.validate_{__name}")
                method(__value)
            except AttributeError as e:
                pass

        self.__dict__[__name] = __value        
        
        # Create an action.
        action = Action(name = "attribute_changed")                       
        # Update the attribute in the database.
        json_value = json.dumps(__value, indent = 4) # Encode the value as json
        sqlquery = f"INSERT INTO research_object_attributes (action_id, object_id, attr_id, attr_value) VALUES ('{action.id}', '{self.id}', '{ResearchObject._get_attr_id(__name, __value)}', '{json_value}')"                
        action.add_sql_query(sqlquery)
        # If the attribute contains the words "current" and "id" and the ID has been validated, add a digraph edge between the two objects.
        if "current" in __name and "id" in __name and validate:
            json_value = json.dumps(DEFAULT_EXISTS_ATTRIBUTE_VALUE, indent = 4)         
            sqlquery = f"INSERT INTO research_object_attributes (action_id, object_id, attr_id, attr_value, target_object_id) VALUES ('{action.id}', '{self.id}', '{ResearchObject._get_attr_id(DEFAULT_EXISTS_ATTRIBUTE_NAME, DEFAULT_EXISTS_ATTRIBUTE_VALUE)}', '{json_value}', '{__value}')"
            action.add_sql_query(sqlquery)
        action.execute()         

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
    def create_id(cls, abstract: str = None, instance: str = None, is_abstract: bool = False) -> str:
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
            if is_abstract:
                instance_new = ""
 
            id = cls.prefix + abstract_new + "_" + instance_new
            cursor = Action.conn.cursor()
            sql = f'SELECT object_id FROM {table_name} WHERE object_id = "{id}"'
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                is_unique = True
            elif is_abstract:
                raise ValueError("Abstract ID already exists.")
        return id      

    @abstractmethod
    def _get_attr_id(attr_name: str, attr_value: Any = None) -> int:
        """Get the ID of an attribute given its name. If it does not exist, create it."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT attr_id FROM Attributes WHERE attr_name = '{attr_name}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        # If the attribute does not exist, create it.
        if len(rows) == 0:
            attr_type = str(type(attr_value)).split("'")[1]
            sqlquery = f"INSERT INTO Attributes (attr_name, attr_type) VALUES ('{attr_name}', '{attr_type}')"
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

    @abstractmethod
    def _get_attr_type(attr_id: int) -> str:
        """Get the type of an attribute given the attribute's ID. If it does not exist, return an error."""
        from pydoc import locate
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT attr_type FROM Attributes WHERE attr_id = '{attr_id}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        if len(rows) == 0:
            raise Exception("No attribute with that ID exists.")
        return locate(rows[0][0])
    
    ###############################################################################################################################
    #################################################### end of abstract methods ##################################################
    ###############################################################################################################################

    def abstract_id(self) -> str:
        """Return the abstract ID of the current object."""
        return self.parse_id(self.id)[1]

    def is_instance_object(self) -> bool:
        """Return true if the object is an instance object, false if it is an abstract object."""
        return self.parse_id(self.id)[2] is not None

    def object_exists(self, id: str) -> bool:
        """Return true if the specified id exists in the database, false if not."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT object_id FROM research_objects WHERE object_id = '{id}'"
        cursor.execute(sqlquery)
        rows = cursor.fetchall()
        return len(rows) > 0
        
    def is_id(self, id: str) -> bool:
        """Check if the given ID matches the pattern of a valid research object ID."""              
        pattern = "^[a-zA-Z]{2}[a-fA-F0-9]{6}_[a-fA-F0-9]{3}$"
        if not isinstance(id, str):
            raise ValueError("id must be a str!")
        if not re.match(pattern, id):
            return False
        return True    

    def _is_id_or_none(self, id: str) -> bool:
        """Check if the given ID matches the pattern of a valid research object ID, or is None."""              
        if id is None or self.is_id(id):
            return True
        return False

    def _get_all_source_object_ids(self, cls) -> list[str]:
        """Get all source object ids of the specified target object of the specified type. Immediate neighbors only, equivalent to predecessors() method"""
        sql = f'SELECT object_id, {DEFAULT_EXISTS_ATTRIBUTE_NAME} FROM research_object_attributes WHERE target_object_id = "{self.id}"'
        return self.__get_all_related_object_ids(cls, sql)
    
    def _get_all_target_object_ids(self, cls) -> list[str]:
        """Get all target object ids of the specified source object of the specified type. Immediate neighbors only, equivalent to successors() method"""
        sql = f'SELECT target_object_id, {DEFAULT_EXISTS_ATTRIBUTE_NAME} FROM research_object_attributes WHERE object_id = "{self.id}"'
        return self.__get_all_related_object_ids(cls, sql)

    def __get_all_related_object_ids(self, cls, sql) -> list[str]:
        """Called by _get_all_source_object_ids and _get_all_target_object_ids.
        Get all related object ids of the specified object of the specified type, either source of target objects."""
        # TODO: Ensure that the edges are not outdated, check the "exists" property.
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
    
    def _add_target_object_id(self, id: str) -> None:
        """Add a target object ID to the current source object in the database."""
        if not self.is_id(id):
            raise ValueError("Invalid ID.")              
        if self._is_target(id):
            return # Already exists.
        sql = f"INSERT INTO research_object_attributes (object_id, target_object_id) VALUES ('{self.id}', '{id}')"
        action = Action(name = "add_target_object_id")
        action.add_sql_query(sql)
        action.execute()

    def _remove_target_object_id(self, id: str) -> None:
        """Remove a target object ID from the current source object."""
        if not self.is_id(id):
            raise ValueError("Invalid ID.")              
        if not self._is_target(id):
            return
        sql = f"INSERT INTO research_object_attributes (object_id, target_object_id) VALUES ('{self.id}', {None})"
        action = Action(name = "remove_target_object_id")
        action.add_sql_query(sql)
        action.execute()

    def _add_source_object_id(self, id: str, cls: type) -> None:
        """Add a source object ID to the current target object in the database."""
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
        """Remove a source object ID from the current target object in the database."""
        if not self._is_id(id):
            raise ValueError("Invalid ID.")      
        self.validate_id_class(id, cls)  
        if not self._is_source(id):
            return
        sql = f"INSERT INTO research_object_attributes (object_id, target_object_id) VALUES ('{id}', {None})"
        action = Action(name = "remove_source_object_id")
        action.add_sql_query(sql)
        action.execute()

    def _gen_obj_or_none(self, ids, cls: type) -> "ResearchObject":
        """Generate the objects of the specified class, or None if the IDs argument is None or []."""
        if ids is None or len(ids) == 0:
            return None
        return [cls(id = id) for id in ids]
    
    ###############################################################################################################################
    #################################################### end of parentage methods #################################################
    ############################################################################################################################### 
        
    def copy_to_new_instance(self, new_id: str = None) -> "ResearchObject":
        """Copy the current object to a new object with a new instance ID but the same abstract ID. Return the new object."""
        cls = type(self)
        if new_id is None:
            abstract_id = self.parse_id(self.id)[1]
            new_id = cls.create_id(cls, abstract = abstract_id)
        new_object = cls(copy = True, id = new_id)        
        attrs = self.__dict__
        for key, value in attrs.items():
            if key == "id":
                continue
            # No validation because that would've already happened when the original object was created.
            new_object.__setattr__(key, value, validate = False)
        return new_object

    ###############################################################################################################################
    ############################################ end of abstract/instance relation methods ########################################
    ###############################################################################################################################     
    
    def parse_id(self, id: str) -> tuple:
        """Parse an ID into its prefix, abstract, and instance parts."""
        if not self.is_id(id):
            raise ValueError("Invalid ID.")
        prefix = id[0:2]
        abstract = id[2:2+abstract_id_len]
        num_underscores = id.count("_")
        instance = None
        if num_underscores == 1:            
            instance = id[-instance_id_len:]
        return (prefix, abstract, instance)
    
if __name__=="__main__":
    # Cannot run anything from here because Action is a circular import.
    pass