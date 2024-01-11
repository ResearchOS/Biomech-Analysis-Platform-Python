from typing import Any
import re
from abc import abstractmethod
import weakref
import json

from ResearchOS.action import Action
from ResearchOS import Config

abstract_id_len = Config.abstract_id_len
instance_id_len = Config.instance_id_len

DEFAULT_EXISTS_ATTRIBUTE_NAME = "exists"
DEFAULT_EXISTS_ATTRIBUTE_VALUE = True
DEFAULT_NAME_ATTRIBUTE_NAME = "name"
DEFAULT_NAME_ATTRIBUTE_VALUE = "object creation" 
DEFAULT_ABSTRACT_KWARG_NAME = "abstract"

class ResearchObject():
    """One research object. Parent class of Data Objects & Pipeline Objects."""

    prefix = "RO" # Testing only
    _objects = weakref.WeakValueDictionary()
    # _objects_count = {}   
    
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

    def __new__(cls, *args, **kwargs):
        """Create a new research object. If the object already exists, return the existing object.
        If abstract is True, returns an abstract object that does not have an instance ID.
        Otherwise, returns an instance object that has an instance ID."""
        if DEFAULT_ABSTRACT_KWARG_NAME not in kwargs.keys():
            abstract = False
        object_id = None
        if len(args)==1:
            object_id = args[0]
        elif len(args) > 1:
            raise ValueError("Only id can be a positional argument")
        if object_id is None:
            object_id = kwargs.get("id", None)        
        if object_id is None:            
            object_id = cls.create_id(cls, is_abstract = abstract)
        if object_id in ResearchObject._objects:
            # ResearchObject._objects_count[object_id] += 1
            return ResearchObject._objects[object_id]
        else: # Create a new object.
            instance = super(ResearchObject, cls).__new__(cls)
            ResearchObject._objects[object_id] = instance
            # ResearchObject._objects_count[object_id] = 1
            instance.__dict__['id'] = object_id
            return instance

    def __init__(self, name: str = DEFAULT_NAME_ATTRIBUTE_NAME, default_attrs: dict = {}, **kwargs) -> None:
        """id is required as either an arg or kwarg but will actually not be used here because it is assigned during __new__"""
        id = self.id # self.id always exists by this point thanks to __new__
        if not self.is_id(id):
            raise ValueError("Not an ID!")
        if "id" in kwargs:
            del kwargs["id"]
        try:
            # Fails if the object does not exist.
            is_new = False
            self.load()
        except ValueError:
            # Create the new object in the database.
            is_new = True
            action = Action(name = name)
            sqlquery = f"INSERT INTO research_objects (object_id) VALUES ('{id}')"
            action.add_sql_query(sqlquery)
            action.execute(commit = False)            
            default_attrs = {**default_attrs, **{DEFAULT_EXISTS_ATTRIBUTE_NAME: DEFAULT_EXISTS_ATTRIBUTE_VALUE, DEFAULT_NAME_ATTRIBUTE_NAME: name}} # Python 3.5 or later
        all_attrs = {**default_attrs, **kwargs} # Append kwargs to default attributes.
        for attr in all_attrs:
            # Skip if it already exists, or if it exists in the object (from being loaded) and is the same as the kwarg's value.
            if attr in self.__dict__ or (attr in kwargs and attr in self.__dict__ and self.__dict__[attr] != kwargs[attr]):
                continue
            validate = False
            if attr in kwargs:
                validate = True
            self.__setattr__(attr, all_attrs[attr], validate = validate)
        if is_new:
            action.execute()

    @abstractmethod
    def _get_time_ordered_result(result: list, action_col_num: int) -> list[str]:
        """Return the result array from conn.cursor().execute() in reverse chronological order (e.g. latest first)."""
        unordered_action_ids = [row[action_col_num] for row in result] # A list of action ID's in no particular order.
        action_ids_str = {','.join([f'"{action_id}"' for action_id in unordered_action_ids])}
        sqlquery = f"SELECT action_id FROM actions WHERE action_id IN ({action_ids_str}) ORDER BY timestamp DESC"
        cursor = Action.conn.cursor()
        ordered_action_ids = cursor.execute(sqlquery).fetchall()
        if ordered_action_ids is None or len(ordered_action_ids) == 0:
            raise ValueError("No actions found.")
        ordered_action_ids = [action_id[0] for action_id in ordered_action_ids]
        indices = [ordered_action_ids.index(action_id) for action_id in unordered_action_ids]
        sorted_result = [result[index] for index in indices]
        return sorted_result

    def load(self) -> None:
        """Load the current state of a research object from the database."""        
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT action_id, attr_id, attr_value, target_object_id FROM research_object_attributes WHERE object_id = '{self.id}'"
        unordered_attr_result = cursor.execute(sqlquery).fetchall()
        ordered_attr_result = ResearchObject._get_time_ordered_result(unordered_attr_result, action_col_num = 0)
        if len(unordered_attr_result) == 0:
            raise ValueError("No object with that ID exists.")         
                             
        curr_obj_attr_ids = [row[1] for row in ordered_attr_result]
        num_attrs = len(list(set(curr_obj_attr_ids))) # Get the number of unique action ID's.
        used_attr_ids = []
        attrs = {}
        attrs["id"] = self.id
        for row in ordered_attr_result:            
            attr_id = row[1]
            attr_value_json = row[2]
            target_object_id = row[3]
            if attr_id in used_attr_ids:
                continue
            else:
                used_attr_ids.append(attr_id)                        

            attr_name = ResearchObject._get_attr_name(attr_id)
            # Translate the attribute from string to the proper type/format.                     
            try:
                from_json_method = eval("self.from_json_" + attr_name)
                attr_value = from_json_method(attr_value_json)
            except AttributeError as e:
                attr_value = json.loads(attr_value_json)            
            # Now that the value is loaded as the proper type/format (and is not None), validate it.
            try:
                if attr_value is not None:
                    validate_method = eval("self.validate_" + attr_name)
                    validate_method(attr_value)
            except AttributeError as e:
                pass
            attrs[attr_name] = attr_value
            if len(used_attr_ids) == num_attrs:
                break # Every attribute is accounted for.
                
        self.__dict__.update(attrs)

    def __setattr__(self, __name: str, __value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attributes of a research object in memory and in the SQL database.
        Validates the attribute if it is a built-in ResearchOS attribute (i.e. a method exists to validate it).
        If it is not a built-in ResearchOS attribute, then no validation occurs."""
        # TODO: Have already implemented adding current_XXX_id object to digraph in the database, but should also update the in-memory digraph.        
        if not validate and self.__dict__.get(__name, None) == __value:
            return # No change.
        if __name == "id":
            raise ValueError("Cannot change the ID of a research object.")
        if __name[0] == "_":
            return # Don't log private attributes.
        # Validate the value        
        if validate:                                                      
            try:
                validate_method = eval(f"self.validate_{__name}")
                validate_method(__value)
            except AttributeError as e:
                pass

        to_json_method = None
        try:
            to_json_method = eval(f"self.to_json_{__name}")
            json_value = to_json_method(__value)
        except AttributeError as e:            
            json_value = json.dumps(__value, indent = 4)
        
        # Create an action.
        execute_action = False
        if action is None:
            execute_action = True
            action = Action(name = "attribute_changed")
        # Update the attribute in the database.
        try:
            assert to_json_method is None # Cannot convert to json AND have a store method. Store method takes precedence.
            method = eval(f"self.store_{__name}")            
            action = method(__value, action = action)
        except AttributeError as e:
            self._default_store_obj_attr(__name, __value, json_value, action = action)            
        # If the attribute contains the words "current" and "id" and the ID has been validated, add a digraph edge between the two objects with an attribute.
        pattern = r"^current_[\w\d]+_id$"
        if re.match(pattern, __name) and validate:
            action = self._default_store_edge_attr(target_object_id = __value, name = __name, value = DEFAULT_EXISTS_ATTRIBUTE_VALUE, action = action)
            # if self.__dict__.get(__name, None) != __value:
            #     execute_action = True # Need to execute an action if adding an edge.
        if execute_action:
            action.execute()
        self.__dict__[__name] = __value

    def _default_store_edge_attr(self, target_object_id: str, name: str, value: Any, action: Action) -> None:
        """Create a digraph edge between the current object and the target object with the specified attribute."""
        json_value = json.dumps(value, indent = 4)
        sqlquery = f"INSERT INTO research_object_attributes (action_id, object_id, attr_id, attr_value, target_object_id) VALUES ('{action.id}', '{self.id}', '{ResearchObject._get_attr_id(name, value)}', '{json_value}', '{target_object_id}')"
        action.add_sql_query(sqlquery)
        return action

    def _default_store_obj_attr(self, __name: str, __value: Any, json_value: Any, action: Action) -> Action:
        """If no store_attr method exists for the object attribute, use this default method."""                                      
        sqlquery = f"INSERT INTO research_object_attributes (action_id, object_id, attr_id, attr_value) VALUES ('{action.id}', '{self.id}', '{ResearchObject._get_attr_id(__name, __value)}', '{json_value}')"                
        action.add_sql_query(sqlquery)
        return action

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
    def _get_attr_id(attr_name: str, attr_value: Any) -> int:
        """Get the ID of an attribute given its name. If it does not exist, create it."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT attr_id FROM Attributes WHERE attr_name = '{attr_name}'"
        cursor.execute(sqlquery)            
        rows = cursor.fetchall()
        if len(rows) > 1:
            raise Exception("More than one attribute with the same name.")
        elif len(rows)==0:
            ResearchObject._create_attr(attr_name, attr_value)
            return ResearchObject._get_attr_id(attr_name, attr_value)
        return rows[0][0]
    
    @abstractmethod
    def _create_attr(attr_name: str, attr_value) -> int:
        """Create a new attribute with the specified name and return its ID."""
        cursor = Action.conn.cursor()
        attr_type = str(type(attr_value)).split("'")[1]
        sqlquery = f"INSERT INTO Attributes (attr_name, attr_type) VALUES {attr_name, attr_type}"
        # sqlquery = f"INSERT INTO Attributes (attr_name, attr_type) VALUES ('{attr_name}', '{attr_type}')"
        cursor.execute(sqlquery)
        return cursor.lastrowid
    
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
        instance_pattern = "^[a-zA-Z]{2}[a-fA-F0-9]{6}_[a-fA-F0-9]{3}$"
        abstract_pattern = "^[a-zA-Z]{2}[a-fA-F0-9]{6}$"
        if not isinstance(id, str):
            raise ValueError("id must be a str!")
        if re.match(instance_pattern, id) or re.match(abstract_pattern, id):
            return True
        return False    
    
    def _is_id_of_class(self, id: str, cls: type) -> bool:
        """True if the ID is of the proper type, False if not."""
        return id.startswith(cls.prefix)

    def _is_id_or_none(self, id: str) -> bool:
        """Check if the given ID matches the pattern of a valid research object ID, or is None."""              
        if id is None or self.is_id(id):
            return True
        return False

    def _get_all_source_object_ids(self, cls) -> list[str]:
        """Get all source object ids of the specified target object of the specified type. Immediate neighbors only, equivalent to predecessors() method"""
        sql = f'SELECT object_id, attr_value FROM research_object_attributes WHERE target_object_id = "{self.id}"'
        return self.__get_all_related_object_ids(cls, sql)
    
    def _get_all_target_object_ids(self, cls) -> list[str]:
        """Get all target object ids of the specified source object of the specified type. Immediate neighbors only, equivalent to successors() method"""
        sql = f'SELECT target_object_id, attr_value FROM research_object_attributes WHERE object_id = "{self.id}"'
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
        sql = f"SELECT object_id FROM research_object_attributes WHERE target_object_id = '{self.id}' AND object_id = '{id}'"
        cursor = Action.conn.cursor()
        cursor.execute(sql)
        return len(cursor.fetchall()) > 0
    
    def _is_target(self, id: str) -> bool:
        """Check if the specified object ID is a target object of the current object."""        
        sql = f"SELECT target_object_id FROM research_object_attributes WHERE object_id = '{self.id}' AND target_object_id = '{id}'"
        cursor = Action.conn.cursor()
        cursor.execute(sql)
        return len(cursor.fetchall()) > 0
    
    def _add_target_object_id(self, id: str, cls: type) -> None:
        """Add a target object ID to the current source object in the database."""
        if not self.is_id(id):
            raise ValueError("Invalid ID.")    
        if not self._is_id_of_class(id, cls):
            raise ValueError("ID is of the wrong class!")          
        if self._is_target(id):
            return # Already exists.
        action = Action(name = "add_target_object_id")
        json_value = json.dumps(True)
        sql = f"INSERT INTO research_object_attributes (action_id, object_id, target_object_id, attr_id, attr_value) VALUES ('{action.id}', '{self.id}', '{id}', {ResearchObject._get_attr_id(DEFAULT_EXISTS_ATTRIBUTE_NAME)}, '{json_value}')"        
        action.add_sql_query(sql)
        action.execute()

    def _remove_target_object_id(self, id: str, cls: type) -> None:
        """Remove a target object ID from the current source object."""
        if not self.is_id(id):
            raise ValueError("Invalid ID.")   
        if not self._is_id_of_class(id, cls):
            raise ValueError("ID is of the wrong class!")           
        if not self._is_target(id):
            return
        action = Action(name = "remove_target_object_id")
        json_value = json.dumps(False)
        sql = f"INSERT INTO research_object_attributes (action_id, object_id, target_object_id, attr_id, attr_value) VALUES ('{action.id}', '{self.id}', {id}, {ResearchObject._get_attr_id(DEFAULT_NAME_ATTRIBUTE_NAME)}, '{json_value}')"        
        action.add_sql_query(sql)
        action.execute()

    def _add_source_object_id(self, id: str, cls: type) -> None:
        """Add a source object ID to the current target object in the database."""
        if not self._is_id(id):
            raise ValueError("Invalid ID.")      
        if not self._is_id_of_class(id, cls):
            raise ValueError("ID is of the wrong class!")
        if self._is_source(id):
            return # Already exists
        action = Action(name = "add_source_object_id")
        json_value = json.dumps(True)
        sql = f"INSERT INTO research_object_attributes (action_id, object_id, target_object_id, attr_id, attr_value) VALUES ('{action.id}', '{id}', '{self.id}, {ResearchObject._get_attr_id(DEFAULT_EXISTS_ATTRIBUTE_NAME)}, '{json_value}')"        
        action.add_sql_query(sql)
        action.execute()

    def _remove_source_object_id(self, id: str, cls: type) -> None:
        """Remove a source object ID from the current target object in the database."""
        if not self._is_id(id):
            raise ValueError("Invalid ID.")      
        if not self._is_id_of_class(id, cls):
            raise ValueError("ID is of the wrong class!")
        if not self._is_source(id):
            return
        json_value = json.dumps(False)
        action = Action(name = "remove_source_object_id")
        sql = f"INSERT INTO research_object_attributes (action_id, object_id, target_object_id, attr_id, attr_value) VALUES ('{action.id}', '{id}', {self.id}, {ResearchObject._get_attr_id(DEFAULT_EXISTS_ATTRIBUTE_NAME)}, '{json_value}')"        
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

    def get_abstract_object(self) -> "ResearchObject":
        """Return the abstract object corresponding to the given the instance object. If abstract is given, return self."""
        if not self.is_instance_object():
            return self
        abstract_id = self.parse_id()[1]
        cls = type(self)
        return cls(id = abstract_id)
        
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
    
    def _open_path(self, path: str) -> None:
        """Open a file or directory in the default application."""
        import os, subprocess
        if os.path.isdir(path):
            subprocess.Popen(["open", path])
        else:
            subprocess.Popen(["open", "-R", path])
    
if __name__=="__main__":
    # Cannot run anything from here because Action is a circular import.
    pass