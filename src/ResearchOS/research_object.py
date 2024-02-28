from typing import Any
import json

from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.action import Action
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.idcreator import IDCreator

all_default_attrs = {}
all_default_attrs["notes"] = None

complex_attrs_list = []

root_data_path = "data"

class ResearchObject():
    """One research object. Parent class of Data Objects & Pipeline Objects."""

    def __deepcopy__(self, memo):
        """Raise an error if attempting to deepcopy this object."""
        raise ValueError("Research objects cannot be deepcopied.")

    def __hash__(self):
        return hash(self.id)
    
    def __eq__(self, other):
        if isinstance(other, ResearchObject):
            return self.id == other.id and self is other
        return False
     
    def __new__(cls, **kwargs):
        """Create a new research object in memory. If the object already exists in memory with this ID, return the existing object."""
        if "id" not in kwargs.keys():
            raise ValueError("id is required as a kwarg")  
        id = kwargs["id"]
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
    
    def __getattribute__(self, name: str) -> Any:
        """Get the value of an attribute. Only does any magic if the attribute exists already and is a VR."""        
        subclasses = ResearchObject.__subclasses__()
        vr_class = [x for x in subclasses if (hasattr(x,"prefix") and x.prefix == "VR")][0]
        try:
            value = super().__getattribute__(name) # Throw the default error.
        except AttributeError as e:
            raise e        
        if isinstance(value, vr_class):
            value = ResearchObjectHandler.load_vr_value(self, value)
        return value
    
    def __setattr__(self, name: str = None, value: Any = None, action: Action = None, all_attrs: DefaultAttrs = None, kwargs_dict: dict = {}) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""
        if hasattr(self, name) and getattr(self, name, None) == value:
            return # No change.
        
        # Ensure that the criteria to set the attribute are met.
        if not str(name).isidentifier():
            raise ValueError(f"{name} is not a valid attribute name.") # Offers some protection for having to eval() the name to get custom function names.        
        if name == "id":
            raise ValueError("Cannot change the ID of a research object.")
        if name == "prefix":
            raise ValueError("Cannot change the prefix of a research object.")
        if name == "name":
            if not str(value).isidentifier():
                raise ValueError(f"name attribute, value: {value} is not a valid attribute name.") 
            
        # Set the attribute. Create Action when __setattr__ is called as the top level.
        if all_attrs is None:
            all_attrs = DefaultAttrs(self)                
        commit = False
        if action is None:
            commit = True
            action = Action(name = "attribute_changed")

        if not kwargs_dict:
            kwargs_dict = {name: value}
        self._setattrs(all_attrs.default_attrs, kwargs_dict, action)

        action.commit = commit
        action.exec = True
        action.execute()     
    
    def __init__(self, action: Action = None, **orig_kwargs):
        """Initialize the research object."""        
        id = orig_kwargs["id"]
        if not IDCreator(None).is_ro_id(id):
            raise ValueError("id is not a valid ID.")
        finish_action = False
        if action is None:
            action = Action(name = "__init__", exec = False) # One data object.
            finish_action = True
        self.__dict__["id"] = orig_kwargs["id"] # Put the ID in the __dict__ so that it is not overwritten by the __setattr__ method.
        del orig_kwargs["id"] # Remove the ID from the kwargs so that it is not set as an attribute.        
        attrs = DefaultAttrs(self) # Get the default attributes for the class.
        default_attrs = attrs.default_attrs
        prev_loaded = self.prev_loaded
        del self.__dict__["prev_loaded"]

        if prev_loaded:
            prev_exists = True
        else:
            prev_exists = ResearchObjectHandler.object_exists(self.id, action)
        if not prev_exists:
            # Create a new object.
            query_name = "robj_exists_insert"
            params = (self.id, action.id)
            action.add_sql_query(id, query_name, params, group_name = "robj_insert")
            kwargs = default_attrs | orig_kwargs # Set defaults, but allow them to be overwritten by the kwargs.
        else:
            kwargs = orig_kwargs # Because the defaults will have all been set, don't include them.
        
        if not prev_loaded and prev_exists:
            # Load the existing object's attributes from the database.
            ResearchObjectHandler._load_ro(self, default_attrs, action)                          

        self._setattrs(default_attrs, kwargs, action)

        # Set the attributes.
        if finish_action:
            action.exec = True
            action.commit = True
            action.execute()

    def _setattrs(self, default_attrs: dict, kwargs: dict, action: Action) -> None:
        """Set the attributes of the object.
        default_attrs: The default attributes of the object.
        orig_kwargs: The original kwargs passed to the object.
        kwargs: The kwargs to be used to set the attributes. A combination of the default attributes and the original kwargs."""
        # 1. Set simple builtin attributes.
        ResearchObjectHandler._set_builtin_attributes(self, default_attrs, kwargs, action)

        # 3. Set VR attributes.        
        vr_attrs = {k: v for k, v in kwargs.items() if k not in default_attrs}
        for vr_name in vr_attrs:
            ResearchObjectHandler._set_vr_attributes(self, vr_name, vr_attrs[vr_name], action)
            # if vr_name == "Side_Of_Interest":
            #     a = getattr(self, vr_name)

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