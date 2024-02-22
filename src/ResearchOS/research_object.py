from typing import Any

from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.action import Action
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.default_attrs import DefaultAttrs

all_default_attrs = {}
all_default_attrs["notes"] = None

complex_attrs_list = []

root_data_path = "data"

class ResearchObject():
    """One research object. Parent class of Data Objects & Pipeline Objects."""

    def __hash__(self):
        return hash(self.id)
    
    def __eq__(self, other):
        if isinstance(other, ResearchObject):
            return self.id == other.id and self is other
        return False
    
    def __getattribute__(self, name: str) -> Any:
        """Get the value of an attribute. Only does any magic if the attribute exists already and is a VR."""
        from ResearchOS.DataObjects.data_object import DataObject
        subclasses = DataObject.__subclasses__()
        vr_class = [x for x in subclasses if x.prefix == "VR"][0]
        try:
            value = super().__getattribute__(name) # Throw the default error.
        except AttributeError as e:
            raise e        
        if isinstance(value, vr_class):
            value = ResearchObjectHandler.load_vr_value(self, value)
        return value
    
    def __setattr__(self, name, value, action: Action = None, validate: bool = True, all_attrs: DefaultAttrs = None) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""
        if hasattr(self, name) and getattr(self, name) == value:
            return # No change.
        if all_attrs is None:
            all_attrs = DefaultAttrs(self)
        if action is None:            
            action = Action(name = "attribute_changed")
        ResearchObjectHandler._setattr(self, name, value, action, validate, all_attrs.default_attrs, all_attrs.complex_attrs)
        if action.do_exec:        
            action.execute()

    def __new__(cls, **kwargs):
        """Create a new research object in memory. If the object already exists in memory with this ID, return the existing object."""
        kwargs = ResearchObjectHandler.check_inputs(cls, kwargs)
        id = kwargs["id"]
        # keys = ",".join([key for key in ResearchObjectHandler.instances.keys()])
        # print("Keys: " + keys)
        if id in ResearchObjectHandler.instances:
            ResearchObjectHandler.counts[id] += 1
            ResearchObjectHandler.instances[id].__dict__["prev_loaded"] = True
            return ResearchObjectHandler.instances[id]
        ResearchObjectHandler.counts[id] = 1
        instance = super(ResearchObject, cls).__new__(cls)
        ResearchObjectHandler.instances[id] = instance
        ResearchObjectHandler.instances[id].__dict__["prev_loaded"] = False
        instance.__dict__["prev_loaded"] = False        
        return instance    
    
    def __init__(self, **orig_kwargs):
        """Initialize the research object."""   
        self.__dict__["id"] = orig_kwargs["id"] # Put the ID in the __dict__ so that it is not overwritten by the __setattr__ method.
        del orig_kwargs["id"] # Remove the ID from the kwargs so that it is not set as an attribute.        
        attrs = DefaultAttrs(self) # Get the default attributes for the class.
        default_attrs = attrs.default_attrs

        # Will be overwritten if creating a new object.
        action = Action(name = f"set object attributes")
        kwargs = orig_kwargs # Because the defaults will have all been set, don't include them.
        prev_exists = ResearchObjectHandler.object_exists(self.id)
        if not self.prev_loaded and prev_exists:
            # Load the existing object's attributes from the database.
            ResearchObjectHandler._load_ro(self, default_attrs)
        elif not prev_exists:
            # Create a new object.
            action = Action(name = f"created object")
            ResearchObjectHandler._create_ro(self, action = action) # Create the object in the database.
            kwargs = default_attrs | orig_kwargs # Set defaults, but allow them to be overwritten by the kwargs.
        del self.__dict__["prev_loaded"] # Remove the prev_loaded attribute from the object.

        # Set the attributes.
        for key in kwargs:
            validate = True # Default is to validate any attribute.        
            # If previously loaded, don't overwrite a default attribute with its default value. If it was specified as a kwarg, then use that specified value.
            if key in self.__dict__ and key not in orig_kwargs:
                continue
            # If the attribute value is a default value, don't validate it.
            if key in default_attrs and kwargs[key] == default_attrs[key]:
                validate = False
            self.__setattr__(key, kwargs[key], action = action, validate = validate, all_attrs = attrs)
            action.execute() # Commit the action to the database.    

    def get_vr(self, name: str) -> Any:
        """Get the VR itself instead of its value."""
        return self.__dict__[name]

    def get_dataset_id(self) -> str:
        """Get the most recent dataset ID."""        
        sqlquery = f"SELECT action_id, dataset_id FROM data_address_schemas"
        pool = SQLiteConnectionPool()
        conn = pool.get_connection()
        cursor = conn.cursor()
        result = cursor.execute(sqlquery).fetchall()
        pool.return_connection(conn)
        # ordered_result = ResearchObjectHandler._get_time_ordered_result(result, action_col_num=0)
        if not result:
            raise ValueError("Need to create a dataset and set up its schema first.")
        dataset_id = result[-1][1]        
        return dataset_id

    def get_current_schema_id(self, dataset_id: str) -> str:
        conn = ResearchObjectHandler.pool.get_connection()
        sqlquery = f"SELECT action_id FROM data_address_schemas WHERE dataset_id = '{dataset_id}'"
        action_ids = conn.cursor().execute(sqlquery).fetchall()
        action_ids = ResearchObjectHandler._get_time_ordered_result(action_ids, action_col_num=0)
        action_id_schema = action_ids[0][0] if action_ids else None
        if action_id_schema is None:
            ResearchObjectHandler.pool.return_connection(conn)
            return # If the schema is empty and the addresses are empty, this is likely initialization so just return.

        sqlquery = f"SELECT schema_id FROM data_address_schemas WHERE dataset_id = '{dataset_id}' AND action_id = '{action_id_schema}'"
        schema_id = conn.execute(sqlquery).fetchone()
        schema_id = schema_id[0] if schema_id else None
        ResearchObjectHandler.pool.return_connection(conn)
        return schema_id