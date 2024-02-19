from typing import Any
import json

from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.DataObjects.data_object import DataObject

all_default_attrs = {}
all_default_attrs["level"] = None
all_default_attrs["hard_coded_value"] = None

complex_attrs_list = []

class Variable(DataObject,  PipelineObject):
    """Variable class."""

    prefix: str = "VR"

    def load(self) -> None:
        """Load the variable-specific attributes from the database in an attribute-specific way."""
        pass
    
    ## Level methods

    def validate_level(self, level: type) -> None:
        """Check that the level is of a valid type."""
        if not isinstance(level, type):
            raise ValueError("Level must be a type.")
        if level not in DataObject.__subclasses__():
            raise ValueError("Level must be a DataObject.")
        
    def to_json_level(self, level: type) -> dict:
        """Return the level as a JSON object."""
        return json.dumps(level.prefix)

    def from_json_level(self, level: str) -> type:
        """Return the level as a JSON object."""
        level_str = json.loads(level)
        subclasses = DataObject.__subclasses__()
        return [cls for cls in subclasses if cls.prefix == level_str][0]
    
    