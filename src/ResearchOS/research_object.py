from typing import Any
import re
from abc import abstractmethod
import weakref
import json

from ResearchOS.action import Action
from ResearchOS import Config
# from idcreator import IDCreator
# from ResearchOS.DBHandlers.db_handler_sqlite import DBHandlerSQLite

config = Config()

abstract_id_len = config.abstract_id_len
instance_id_len = config.instance_id_len

DEFAULT_EXISTS_ATTRIBUTE_NAME = "exists"
DEFAULT_EXISTS_ATTRIBUTE_VALUE = True
DEFAULT_NAME_ATTRIBUTE_NAME = "name"
DEFAULT_NAME_ATTRIBUTE_VALUE = "object creation" 
DEFAULT_ABSTRACT_KWARG_NAME = "abstract"

DEFAULT_USER_ID = "US000000_000"

# DEFAULT_USER_PARENT = "US000000_000"

ok_parent_class_prefixes = ["US", "DS", "PJ", "AN"] # The list of classes that have a "current_{parent}_id" builtin method (or no parent, for User).

class ResearchObject():
    """One research object. Parent class of Data Objects & Pipeline Objects."""

    prefix = "RO" # Testing only
    _objects = weakref.WeakValueDictionary()
    _current_source_type_prefix = None # Overwritten by subclasses that need it. For builtin "current_{cls}_id" attributes.
    _source_type_prefix = None # Overwritten by all subclasses except User. For knowing which classes are valid target types.

    def load(self) -> None:
        """Load "simple" attributes from the database."""
        # 1. Get the database cursor.

        # 2. Get the attributes from the database.
        # Make sure to only get the attributes from action ID's that have not been overwritten.

        # 3. Set the attributes of the object.

    def new(self) -> None:
        """Initialize the class-independent attributes of a new research object."""
        self.name = "Default Name"
    
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
            raise ValueError("id is required as either an arg or kwarg") # Temporary - not inherently necessary, but maybe should be when running headless for idempotency?
            # object_id = cls.create_id(cls, is_abstract = abstract) # Uncomment if/when 0 args/kwargs in constructor is ok.
        if object_id in ResearchObject._objects:
            # ResearchObject._objects_count[object_id] += 1
            return ResearchObject._objects[object_id]
        else: # Create a new object.
            instance = super(ResearchObject, cls).__new__(cls)
            ResearchObject._objects[object_id] = instance
            # ResearchObject._objects_count[object_id] = 1
            instance.__dict__['id'] = object_id
            return instance

    def __init__(self, name: str = DEFAULT_NAME_ATTRIBUTE_NAME, default_attrs: dict = {}, action: Action = None, **kwargs) -> None:
        """id is required as either an arg or kwarg but will actually not be used here because it is assigned during __new__().
        action is only ever an input during initialization."""
        import ResearchOS as ros
        id = self.id # self.id always exists by this point thanks to __new__()
        if not self.is_id(id):
            raise ValueError("Not an ID!")
        # 1. Determine whether we are creating a new object or loading an object from the database.
        is_new = False
        if self.object_exists(id):
            is_new = True

        # 2. Load the object from the database, if it already exists.
        # Need to leave room for arguments in the constructor to overwrite existing attributes (or create new ones).
        if not is_new:
            self.load() # Now all attributes are populated.
        else:
            pass




        
        action = Action(name = name)
        
        if "id" in kwargs:
            del kwargs["id"]
        if "parent" not in kwargs:
            # TODO: Opportunity to use better software design here. This is a hack.
            if self._current_source_type_prefix is not None and not isinstance(self, ros.User):
                # Need to auto-add the parent if it is not specified for the classes that support that.
                cls = self._prefix_to_class(self._current_source_type_prefix)
                us = ros.User(id = ros.User.get_current_user_object_id())
                if cls is ros.User:
                    parent = us
                else:
                    pj = ros.Project(id = us.current_project_id)
                    if cls is ros.Project:
                        parent = pj
                    else:
                        an = ros.Analysis(id = pj.current_analysis_id)
                        ds = ros.Dataset(id = pj.current_dataset_id)
                        if cls is ros.Analysis:
                            parent = an
                        elif cls is ros.Dataset:
                            parent = ds                                                
            elif not isinstance(self, ros.User):
                raise ValueError("parent is required as a kwarg")
            else:
                parent = None
        else:
            parent = kwargs["parent"]
        if isinstance(parent, ResearchObject):
            parent = parent.id
            kwargs["parent"] = parent
        if parent is not None and not self.object_exists(parent):
            raise ValueError("parent is not a valid, pre-existing object ID!")        
        if self.object_exists(id = id):
            is_new = False
            self.load()
        else:
            # Create the new object in the database.
            is_new = True            
            sqlquery = f"INSERT INTO research_objects (object_id) VALUES ('{id}')"
            if id is not DEFAULT_USER_ID: # Don't add the default user to the database, it's already in there.
                action.add_sql_query(sqlquery)
            action.execute(commit = False)            
            default_attrs = {**default_attrs, **{DEFAULT_EXISTS_ATTRIBUTE_NAME: DEFAULT_EXISTS_ATTRIBUTE_VALUE, DEFAULT_NAME_ATTRIBUTE_NAME: name}} # Python 3.5 or later
        all_attrs = {**default_attrs, **kwargs} # Append kwargs to default attributes. Overwrites default attributes with same key.

        for attr in all_attrs:
            validate = True
            set_attr_flag = False
            if attr in default_attrs:
                if attr not in self.__dict__:
                    set_attr_flag = True
                if attr not in kwargs:
                    validate = False
            if attr in kwargs:
                if attr not in default_attrs:
                    validate = False
                if attr not in self.__dict__:                    
                    set_attr_flag = True
                elif self.__dict__[attr] != kwargs[attr]:
                    set_attr_flag = True
            if set_attr_flag:
                self.__setattr__(attr, all_attrs[attr], action = action, validate = validate)
        # If the parent is not an existing parent, then add it as a parent.
        if parent and not self._is_source(parent):
            self._add_source_object_id(parent)
        if is_new:
            action.execute()

    @abstractmethod
    def _get_time_ordered_result(result: list, action_col_num: int) -> list[str]:
        """Return the result array from conn.cursor().execute() in reverse chronological order (e.g. latest first)."""
        unordered_action_ids = [row[action_col_num] for row in result] # A list of action ID's in no particular order.
        action_ids_str = ', '.join([f'"{action_id}"' for action_id in unordered_action_ids])
        sqlquery = f"SELECT action_id FROM actions WHERE action_id IN ({action_ids_str}) ORDER BY timestamp DESC"
        cursor = Action.conn.cursor()
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

    def load(self) -> None:
        """Load the current state of a research object from the database."""
        # 1. Get all of the attributes from the research_object_attributes table.

        # 2. If DataObject, load the data values from the data_values table.
        

        # 3. If load_type_attrs() method exists, run that to load attributes in a type-specific way.



        # cursor = Action.conn.cursor()
        # sqlquery = f"SELECT action_id, attr_id, attr_value, target_object_id FROM research_object_attributes WHERE object_id = '{self.id}'"
        # unordered_attr_result = cursor.execute(sqlquery).fetchall()
        # ordered_attr_result = ResearchObject._get_time_ordered_result(unordered_attr_result, action_col_num = 0)
        # if len(unordered_attr_result) == 0:
        #     raise ValueError("No object with that ID exists.")         
                             
        # curr_obj_attr_ids = [row[1] for row in ordered_attr_result]
        # num_attrs = len(list(set(curr_obj_attr_ids))) # Get the number of unique action ID's.
        # used_attr_ids = []
        # attrs = {}
        # attrs["id"] = self.id
        # for row in ordered_attr_result:            
        #     attr_id = row[1]
        #     attr_value_json = row[2]
        #     target_object_id = row[3]
        #     if attr_id in used_attr_ids:
        #         continue
        #     else:
        #         used_attr_ids.append(attr_id)                        

        #     attr_name = ResearchObject._get_attr_name(attr_id)
        #     # Translate the attribute from string to the proper type/format.                     
        #     try:
        #         from_json_method = eval("self.from_json_" + attr_name)
        #         attr_value = from_json_method(attr_value_json)
        #     except AttributeError as e:
        #         attr_value = json.loads(attr_value_json)

        #     try:
        #         method = eval(f"self.load_{attr_name}")            
        #         method(attr_value)
        #     except AttributeError as e:
        #         pass
        #     # Now that the value is loaded as the proper type/format (and is not None), validate it.
        #     try:
        #         if attr_value is not None:
        #             validate_method = eval("self.validate_" + attr_name)
        #             validate_method(attr_value)
        #     except AttributeError as e:
        #         pass
        #     attrs[attr_name] = attr_value
        #     if len(used_attr_ids) == num_attrs:
        #         break # Every attribute is accounted for.
                
        # self.__dict__.update(attrs)

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attributes of a research object in memory and in the SQL database.
        Validates the attribute if it is a built-in ResearchOS attribute (i.e. a method exists to validate it).
        If it is not a built-in ResearchOS attribute, then no validation occurs."""
        # TODO: Have already implemented adding current_XXX_id object to digraph in the database, but should also update the in-memory digraph.        
        if name in self.__dict__ and self.__dict__.get(name, None) == value:
            return # No change.
        if name == "id":
            raise ValueError("Cannot change the ID of a research object.")
        if name[0] == "_":
            return # Don't log private attributes.
        # Validate the value        
        if validate:                                                      
            try:
                validate_method = eval(f"self.validate_{name}")
                validate_method(value)
            except AttributeError as e:
                pass

        # Create an action.
        execute_action = False
        if action is None:
            execute_action = True
            action = Action(name = "attribute_changed")
        to_json_method = None
        try:
            to_json_method = eval(f"self.to_json_{name}")
            json_value = to_json_method(value, action = action)
        except AttributeError as e:
            json_value = json.dumps(value, indent = 4)
                
        # Update the attribute in the database.
        try:
            # assert to_json_method is None # Cannot convert to json AND have a store method. Store method takes precedence.
            method = eval(f"self.store_{name}")            
            method(value, action = action)
            execute_action = True # Just in case.
        except AttributeError as e:
            self._default_store_obj_attr(name, value, json_value, action = action)            
        # If the attribute contains the words "current" and "id" and the ID has been validated, add a digraph edge between the two objects with an attribute.
        pattern = r"^current_[\w\d]+_id$"
        if re.match(pattern, name):
            action = self._default_store_edge_attr(target_object_id = value, name = name, value = DEFAULT_EXISTS_ATTRIBUTE_VALUE, action = action)
            # if self.__dict__.get(name, None) != value:
            #     execute_action = True # Need to execute an action if adding an edge.
        if execute_action:
            action.execute()
        self.__dict__[name] = value

    def _default_store_edge_attr(self, target_object_id: str, name: str, value: Any, action: Action) -> None:
        """Create a digraph edge between the current object and the target object with the specified attribute."""
        json_value = json.dumps(value, indent = 4)
        sqlquery = f"INSERT INTO research_object_attributes (action_id, object_id, attr_id, attr_value, target_object_id) VALUES ('{action.id}', '{self.id}', '{ResearchObject._get_attr_id(name, value)}', '{json_value}', '{target_object_id}')"
        action.add_sql_query(sqlquery)
        return action

    def _default_store_obj_attr(self, name: str, value: Any, json_value: Any, action: Action) -> Action:
        """If no store_attr method exists for the object attribute, use this default method."""                                      
        sqlquery = f"INSERT INTO research_object_attributes (action_id, object_id, attr_id, attr_value) VALUES ('{action.id}', '{self.id}', '{ResearchObject._get_attr_id(name, value)}', '{json_value}')"                
        action.add_sql_query(sqlquery)
        return action  

    def _get_subclasses(self, cls):
        """Recursively get all subclasses of the provided class
        Self argument is ignored."""        
        subclasses = cls.__subclasses__()
        result = subclasses[:]
        for subclass in subclasses:
            result.extend(self._get_subclasses(subclass))
        return result
    
    def _prefix_to_class(self, prefix: str) -> type:
        """Convert a prefix to a class."""
        for cls in self._get_subclasses(ResearchObject):
            if cls.prefix == prefix:
                return cls
        raise ValueError("No class with that prefix exists.")
    
    def _is_orphan_with_removal(self, id: str) -> bool:
        """Check if the object would be orphaned if the specified object ID were removed from its list of parent ID's."""
        if not self.is_id(id):
            raise ValueError("Invalid ID.")
        if not self._is_source(id):
            raise ValueError("ID is not a source object of the current object.")
        if len(self._get_all_source_object_ids(type(self))) == 1:
            return True

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
        config = Config()
        db_handler = DBHandlerSQLite(config.db_file_path)
        id_creator = IDCreator(db_handler)
        return id_creator.create_ro_id(cls, abstract = abstract, instance = instance, is_abstract = is_abstract)
        # import random
        # table_name = "research_objects"
        # is_unique = False
        # while not is_unique:
        #     if not abstract:
        #         abstract_new = str(hex(random.randrange(0, 16**abstract_id_len))[2:]).upper()
        #         abstract_new = "0" * (abstract_id_len-len(abstract_new)) + abstract_new
        #     else:
        #         abstract_new = abstract
            
        #     if not instance:
        #         instance_new = str(hex(random.randrange(0, 16**instance_id_len))[2:]).upper()
        #         instance_new = "0" * (instance_id_len-len(instance_new)) + instance_new
        #     else:
        #         instance_new = instance
        #     if is_abstract:
        #         instance_new = ""
 
        #     id = cls.prefix + abstract_new + "_" + instance_new
        #     cursor = Action.conn.cursor()
        #     sql = f'SELECT object_id FROM {table_name} WHERE object_id = "{id}"'
        #     cursor.execute(sql)
        #     rows = cursor.fetchall()
        #     if len(rows) == 0:
        #         is_unique = True
        #     elif is_abstract:
        #         raise ValueError("Abstract ID already exists.")
        # return id      

    @abstractmethod
    def _get_attr_id(attr_name: str, attr_value: Any) -> int:
        """Get the ID of an attribute given its name. If it does not exist, create it."""
        conn = Action.conn
        cursor = conn.cursor()
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
        sql = f'SELECT target_object_id, attr_value FROM research_object_attributes WHERE object_id = "{self.id} AND target_object_id IS NOT NULL"'
        return self.__get_all_related_object_ids(cls, sql)

    def __get_all_related_object_ids(self, cls, sql) -> list[str]:
        """Called by _get_all_source_object_ids and _get_all_target_object_ids.
        Get all related object ids of the specified object of the specified type, either source of target objects."""
        # TODO: Ensure that the edges are not outdated, check the "exists" property of the edge.
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
        sql = f"INSERT INTO research_object_attributes (action_id, object_id, target_object_id, attr_id, attr_value) VALUES ('{action.id}', '{self.id}', '{id}', {ResearchObject._get_attr_id(DEFAULT_EXISTS_ATTRIBUTE_NAME, True)}, '{json_value}')"        
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

    def _add_source_object_id(self, id: str) -> None:
        """Add a source object ID to the current target object in the database.
        NOTE: I really don't like the way that this method came out. Too many exceptions for too many classes."""           
        if not self.is_id(id):
            raise ValueError("Invalid ID.")
        
        prefix = self.parse_id(id)[0]
        classes = self._get_subclasses(ResearchObject)
        cls = None
        for c in classes:
            if c.prefix == prefix:
                cls = c
                break
        source_obj = cls(id = id)
        attr_name = "current_" + cls.__name__.lower() + "_id"
        if prefix not in self._current_source_type_prefixes:
            raise ValueError("ID is of the wrong class, cannot be linked to this class type!")        
        source_obj.__setattr__(attr_name, self.id)
        if prefix not in self._source_type_prefixes:
            raise ValueError("ID is of the wrong class, cannot be linked to this class type!")

        if self._is_source(id):
            return # Already exists
        action = Action(name = "add_source_object_id")
        json_value = json.dumps(True)
        sql = f"INSERT INTO research_object_attributes (action_id, object_id, target_object_id, attr_id, attr_value) VALUES ('{action.id}', '{id}', '{self.id}, {ResearchObject._get_attr_id(DEFAULT_EXISTS_ATTRIBUTE_NAME)}, '{json_value}')"        
        action.add_sql_query(sql)
        action.execute()

    def _remove_source_object_id(self, id: str) -> None:
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