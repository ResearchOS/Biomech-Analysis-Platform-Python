import json

from ResearchOS.research_object import ResearchObject

all_default_attrs = {}
all_default_attrs["level"] = None
all_default_attrs["hard_coded_value"] = None

complex_attrs_list = []

class Variable(ResearchObject):
    """Variable class."""

    prefix: str = "VR"
    
    ## Level methods

    def validate_level(self, level: type) -> None:
        """Check that the level is of a valid type."""
        from ResearchOS.DataObjects.data_object import DataObject
        if not isinstance(level, type):
            raise ValueError("Level must be a type.")
        if level not in DataObject.__subclasses__():
            raise ValueError("Level must be a DataObject.")
        
    def to_json_level(self, level: type) -> dict:
        """Return the level as a JSON object."""
        if level is None:
            return json.dumps(level)
        return json.dumps(level.prefix)

    def from_json_level(self, level: str) -> type:
        """Return the level as a JSON object."""
        from ResearchOS.DataObjects.data_object import DataObject
        level_str = json.loads(level)
        if level_str is None:
            return level_str
        subclasses = DataObject.__subclasses__()
        return [cls for cls in subclasses if cls.prefix == level_str][0]
    
    