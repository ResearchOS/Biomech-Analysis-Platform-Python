from typing import Any
import json

from ResearchOS.research_object import ResearchObject
from ResearchOS.action import Action

all_default_attrs = {}
all_default_attrs["level"] = None
all_default_attrs["hard_coded_value"] = None

computer_specific_attr_names = []

class Variable(ResearchObject):
    """Variable class."""

    prefix: str = "VR"
    _initialized: bool = False

    def __init__(self, level: ResearchObject = all_default_attrs["level"],
                hard_coded_value: Any = all_default_attrs["hard_coded_value"], 
                **kwargs):
        if self._initialized:
            return
        self.level = level
        self.hard_coded_value = hard_coded_value
        super().__init__(**kwargs)        
    
    ## Level methods

    def validate_level(self, level: type, action: Action, default: Any) -> None:
        """Check that the level is of a valid type."""
        if level == default:
            return
        from ResearchOS.DataObjects.data_object import DataObject
        if not isinstance(level, type):
            raise ValueError("Level must be a type.")
        if level not in DataObject.__subclasses__():
            raise ValueError("Level must be a DataObject.")
        
    def to_json_level(self, level: type, action: Action) -> dict:
        """Return the level as a JSON object."""
        if level is None:
            return json.dumps(level)
        return json.dumps(level.prefix)

    def from_json_level(self, level: str, action: Action) -> type:
        """Return the level as a JSON object."""
        from ResearchOS.DataObjects.data_object import DataObject
        level_str = json.loads(level)
        if level_str is None:
            return level_str
        subclasses = DataObject.__subclasses__()
        return [cls for cls in subclasses if cls.prefix == level_str][0]
    
    