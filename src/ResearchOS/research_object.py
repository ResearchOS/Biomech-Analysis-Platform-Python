from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.action import Action

all_default_attrs = {}
all_default_attrs["name"] = None
all_default_attrs["notes"] = None

# DEFAULT_USER_ID = "US000000_000"

# ok_parent_class_prefixes = ["US", "DS", "PJ", "AN"] # The list of classes that have a "current_{parent}_id" builtin method (or no parent, for User).

class ResearchObject():
    """One research object. Parent class of Data Objects & Pipeline Objects."""

    def __hash__(self):
        return hash(self.id)
    
    def __eq__(self, other):
        if isinstance(other, ResearchObject):
            return self.id == other.id
        return NotImplemented

    def __new__(cls, **kwargs):
        """Create a new research object in memory. If the object already exists in memory with this ID, return the existing object."""
        kwargs = ResearchObjectHandler.check_inputs(kwargs)
        id = kwargs["id"]
        if id in ResearchObjectHandler.instances:
            ResearchObjectHandler.counts[id] += 1
            return ResearchObjectHandler.instances[id]
        ResearchObjectHandler.counts[id] = 1
        instance = super(ResearchObject, cls).__new__(cls)
        ResearchObjectHandler.instances[id] = instance
        return instance
    
    def __init__(self, orig_default_attrs: dict, **orig_kwargs):
        """Initialize the research object."""
        action = None # Initialize the action.
        self.__dict__["id"] = orig_kwargs["id"] # Put the ID in the __dict__ so that it is not overwritten by the __setattr__ method.
        del orig_kwargs["id"]
        default_attrs = all_default_attrs | orig_default_attrs # Merge the default attributes, with class-specific attributes taking precedence (if any conflict)
        if ResearchObjectHandler.object_exists(self.id):
            # Load the existing object's attributes from the database.
            ResearchObjectHandler._load_ro(self, default_attrs) 
            action = Action(name = f"set object attributes")
            kwargs = {}
        else:
            # Create a new object.
            action = Action(name = f"created object")
            ResearchObjectHandler._create_ro(self, action = action) # Create the object in the database.
            # Add the default attributes to the kwargs to be set, only if they're not being overwritten by a kwarg.
            kwargs = default_attrs | orig_kwargs
        for key in kwargs:
            validate = True # Default is to validate any attribute.
            # If the attribute value is a default value, don't validate it.
            if key in default_attrs and kwargs[key] == default_attrs[key]:
                validate = False
            self.__setattr__(key, kwargs[key], action = action, validate = validate)
        action.execute(commit = True)    





        


#     def _default_store_edge_attr(self, target_object_id: str, name: str, value: Any, action: Action) -> None:
#         """Create a digraph edge between the current object and the target object with the specified attribute."""
#         json_value = json.dumps(value, indent = 4)
#         sqlquery = f"INSERT INTO research_object_attributes (action_id, object_id, attr_id, attr_value, target_object_id) VALUES ('{action.id}', '{self.id}', '{ResearchObject._get_attr_id(name, value)}', '{json_value}', '{target_object_id}')"
#         action.add_sql_query(sqlquery)
#         return action

    
#     def _is_orphan_with_removal(self, id: str) -> bool:
#         """Check if the object would be orphaned if the specified object ID were removed from its list of parent ID's."""
#         if not self.is_id(id):
#             raise ValueError("Invalid ID.")
#         if not self._is_source(id):
#             raise ValueError("ID is not a source object of the current object.")
#         if len(self._get_all_source_object_ids(type(self))) == 1:
#             return True

#     ###############################################################################################################################
#     #################################################### end of dunder methods ####################################################
#     ###############################################################################################################################

#     @abstractmethod
#     def get_all_ids(cls) -> list[str]:
#         """Get all object IDs of the specified class."""
#         cursor = Action.conn.cursor()
#         sqlquery = "SELECT object_id FROM research_objects"
#         cursor.execute(sqlquery)
#         rows = cursor.fetchall()
#         return [row[0] for row in rows if (row[0] is not None and row[0].startswith(cls.prefix))] 
    
#     ###############################################################################################################################
#     #################################################### end of abstract methods ##################################################
#     ###############################################################################################################################

#     def abstract_id(self) -> str:
#         """Return the abstract ID of the current object."""
#         return self.parse_id(self.id)[1]

#     def is_instance_object(self) -> bool:
#         """Return true if the object is an instance object, false if it is an abstract object."""
#         return self.parse_id(self.id)[2] is not None
    
#     def _is_id_of_class(self, id: str, cls: type) -> bool:
#         """True if the ID is of the proper type, False if not."""
#         return id.startswith(cls.prefix)

#     def _is_id_or_none(self, id: str) -> bool:
#         """Check if the given ID matches the pattern of a valid research object ID, or is None."""              
#         if id is None or self.is_id(id):
#             return True
#         return False

#     def _get_all_source_object_ids(self, cls) -> list[str]:
#         """Get all source object ids of the specified target object of the specified type. Immediate neighbors only, equivalent to predecessors() method"""
#         sql = f'SELECT object_id, attr_value FROM research_object_attributes WHERE target_object_id = "{self.id}"'
#         return self.__get_all_related_object_ids(cls, sql)
    
#     def _get_all_target_object_ids(self, cls) -> list[str]:
#         """Get all target object ids of the specified source object of the specified type. Immediate neighbors only, equivalent to successors() method"""
#         sql = f'SELECT target_object_id, attr_value FROM research_object_attributes WHERE object_id = "{self.id} AND target_object_id IS NOT NULL"'
#         return self.__get_all_related_object_ids(cls, sql)

#     def __get_all_related_object_ids(self, cls, sql) -> list[str]:
#         """Called by _get_all_source_object_ids and _get_all_target_object_ids.
#         Get all related object ids of the specified object of the specified type, either source of target objects."""
#         # TODO: Ensure that the edges are not outdated, check the "exists" property of the edge.
#         cursor = Action.conn.cursor()
#         cursor.execute(sql)
#         data = []
#         for row in cursor:
#             if row[0] is None:
#                 continue
#             if row[0].startswith(cls.prefix):                
#                 data.append(row[0])
#         return data
    
#     def _is_source(self, id: str) -> bool:
#         """Check if the specified object ID is a source object of the current object."""        
#         sql = f"SELECT object_id FROM research_object_attributes WHERE target_object_id = '{self.id}' AND object_id = '{id}'"
#         cursor = Action.conn.cursor()
#         cursor.execute(sql)
#         return len(cursor.fetchall()) > 0
    
#     def _is_target(self, id: str) -> bool:
#         """Check if the specified object ID is a target object of the current object."""        
#         sql = f"SELECT target_object_id FROM research_object_attributes WHERE object_id = '{self.id}' AND target_object_id = '{id}'"
#         cursor = Action.conn.cursor()
#         cursor.execute(sql)
#         return len(cursor.fetchall()) > 0
    
#     def _add_target_object_id(self, id: str, cls: type) -> None:
#         """Add a target object ID to the current source object in the database."""
#         if not self.is_id(id):
#             raise ValueError("Invalid ID.")    
#         if not self._is_id_of_class(id, cls):
#             raise ValueError("ID is of the wrong class!")          
#         if self._is_target(id):
#             return # Already exists.
#         action = Action(name = "add_target_object_id")
#         json_value = json.dumps(True)
#         sql = f"INSERT INTO research_object_attributes (action_id, object_id, target_object_id, attr_id, attr_value) VALUES ('{action.id}', '{self.id}', '{id}', {ResearchObject._get_attr_id(DEFAULT_EXISTS_ATTRIBUTE_NAME, True)}, '{json_value}')"        
#         action.add_sql_query(sql)
#         action.execute()

#     def _remove_target_object_id(self, id: str, cls: type) -> None:
#         """Remove a target object ID from the current source object."""
#         if not self.is_id(id):
#             raise ValueError("Invalid ID.")   
#         if not self._is_id_of_class(id, cls):
#             raise ValueError("ID is of the wrong class!")           
#         if not self._is_target(id):
#             return
#         action = Action(name = "remove_target_object_id")
#         json_value = json.dumps(False)
#         sql = f"INSERT INTO research_object_attributes (action_id, object_id, target_object_id, attr_id, attr_value) VALUES ('{action.id}', '{self.id}', {id}, {ResearchObject._get_attr_id(DEFAULT_NAME_ATTRIBUTE_NAME)}, '{json_value}')"        
#         action.add_sql_query(sql)
#         action.execute()   

#     def _add_source_object_id(self, id: str) -> None:
#         """Add a source object ID to the current target object in the database.
#         NOTE: I really don't like the way that this method came out. Too many exceptions for too many classes."""           
#         if not self.is_id(id):
#             raise ValueError("Invalid ID.")
        
#         prefix = self.parse_id(id)[0]
#         classes = self._get_subclasses(ResearchObject)
#         cls = None
#         for c in classes:
#             if c.prefix == prefix:
#                 cls = c
#                 break
#         source_obj = cls(id = id)
#         attr_name = "current_" + cls.__name__.lower() + "_id"
#         if prefix not in self._current_source_type_prefixes:
#             raise ValueError("ID is of the wrong class, cannot be linked to this class type!")        
#         source_obj.__setattr__(attr_name, self.id)
#         if prefix not in self._source_type_prefixes:
#             raise ValueError("ID is of the wrong class, cannot be linked to this class type!")

#         if self._is_source(id):
#             return # Already exists
#         action = Action(name = "add_source_object_id")
#         json_value = json.dumps(True)
#         sql = f"INSERT INTO research_object_attributes (action_id, object_id, target_object_id, attr_id, attr_value) VALUES ('{action.id}', '{id}', '{self.id}, {ResearchObject._get_attr_id(DEFAULT_EXISTS_ATTRIBUTE_NAME)}, '{json_value}')"        
#         action.add_sql_query(sql)
#         action.execute()

#     def _remove_source_object_id(self, id: str) -> None:
#         """Remove a source object ID from the current target object in the database."""
#         if not self._is_id(id):
#             raise ValueError("Invalid ID.")      
#         if not self._is_id_of_class(id, cls):
#             raise ValueError("ID is of the wrong class!")
#         if not self._is_source(id):
#             return
#         json_value = json.dumps(False)
#         action = Action(name = "remove_source_object_id")
#         sql = f"INSERT INTO research_object_attributes (action_id, object_id, target_object_id, attr_id, attr_value) VALUES ('{action.id}', '{id}', {self.id}, {ResearchObject._get_attr_id(DEFAULT_EXISTS_ATTRIBUTE_NAME)}, '{json_value}')"        
#         action.add_sql_query(sql)
#         action.execute()

#     def _gen_obj_or_none(self, ids, cls: type) -> "ResearchObject":
#         """Generate the objects of the specified class, or None if the IDs argument is None or []."""
#         if ids is None or len(ids) == 0:
#             return None
#         return [cls(id = id) for id in ids]
    
#     ###############################################################################################################################
#     #################################################### end of parentage methods #################################################
#     ############################################################################################################################### 

#     def get_abstract_object(self) -> "ResearchObject":
#         """Return the abstract object corresponding to the given the instance object. If abstract is given, return self."""
#         if not self.is_instance_object():
#             return self
#         abstract_id = self.parse_id()[1]
#         cls = type(self)
#         return cls(id = abstract_id)
        
#     def copy_to_new_instance(self, new_id: str = None) -> "ResearchObject":
#         """Copy the current object to a new object with a new instance ID but the same abstract ID. Return the new object."""
#         cls = type(self)
#         if new_id is None:
#             abstract_id = self.parse_id(self.id)[1]
#             new_id = cls.create_id(cls, abstract = abstract_id)
#         new_object = cls(copy = True, id = new_id)        
#         attrs = self.__dict__
#         for key, value in attrs.items():
#             if key == "id":
#                 continue
#             # No validation because that would've already happened when the original object was created.
#             new_object.__setattr__(key, value, validate = False)
#         return new_object

#     ###############################################################################################################################
#     ############################################ end of abstract/instance relation methods ########################################
#     ###############################################################################################################################     
    
#     def _open_path(self, path: str) -> None:
#         """Open a file or directory in the default application."""
#         import os
#         import subprocess
#         if os.path.isdir(path):
#             subprocess.Popen(["open", path])
#         else:
#             subprocess.Popen(["open", "-R", path])
    
# if __name__=="__main__":
#     # Cannot run anything from here because Action is a circular import.
#     pass