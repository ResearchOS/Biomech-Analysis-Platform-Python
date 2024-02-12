from typing import Any

import ResearchOS as ros
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.db_connection_factory import DBConnectionFactory

all_default_attrs = {}
all_default_attrs["hard_coded_value"] = None
all_default_attrs["level"] = None # Eventually needs to be a complex attr.

complex_attrs_list = []

class Variable(ros.DataObject, ros.PipelineObject):
    """Variable class."""

    prefix: str = "VR"
    
    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        super().__init__(all_default_attrs, **kwargs)

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""
        if name is "vr":
            raise ValueError("Cannot set 'vr' attribute for a Variable.")
        if name is not "value":
            ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)
        # Set variable value.
        
            

    def load(self) -> None:
        """Load the variable-specific attributes from the database in an attribute-specific way."""
        pass
    
    #################### Start class-specific attributes ###################
    def validate_level(self, level: type) -> None:
        """Check that the level is of a valid type."""
        if not isinstance(level, type):
            raise ValueError("Level must be a type.")
        if not isinstance(level, DataObject):
            raise ValueError("Level must be a DataObject.")
        us = ros.User(id = ros.User.get_current_user_object_id())
        us.validate_current_project_id(id = us.current_project_id)
        pj = ros.Project(id = us.current_project_id)
        pj.validate_current_dataset_id(id = pj.current_dataset_id)
        ds = ros.Dataset(id = pj.current_dataset_id)
        ds.validate_schema(schema = ds.schema)
        if level not in ds.schema:
            raise ValueError("Level must be in the dataset schema.")

    #################### Start Source objects ####################
    def get_source_object_ids(self, cls: type) -> list:
        """Return a list of all source objects for the Variable."""
        ids = self._get_all_source_object_ids(cls = cls)
        return [cls(id = id) for id in ids]
    
    ## NOTE: THERE ARE NO TARGET OBJECTS FOR VARIABLES