from typing import Any

import ResearchOS as ros
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.db_connection_factory import DBConnectionFactory

all_default_attrs = {}

complex_attrs_list = []

class Plot(ros.PipelineObject):
    
    prefix = "PL"    

    # TODO: Plot name/other metadata for saving.
    # TODO: For variables, need to allow ability to specify which analysis to pull from.

    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        super().__init__(all_default_attrs, **kwargs)

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""
        if name == "vr":
            raise ValueError("The attribute 'vr' is not allowed to be set for Pipeline Objects.")
        else:
            ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)

    def load(self) -> None:
        """Load the dataset-specific attributes from the database in an attribute-specific way."""
        ros.PipelineObject.load(self) # Load the attributes specific to it being a PipelineObject.