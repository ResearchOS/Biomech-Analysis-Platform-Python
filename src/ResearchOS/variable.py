from typing import Any

from ResearchOS.research_object import ResearchObject

all_default_attrs = {}
all_default_attrs["hard_coded_value"] = None

computer_specific_attr_names = []

class Variable(ResearchObject):
    """Variable class."""

    prefix: str = "VR"
    _initialized: bool = False

    def __getitem__(self, key: str) -> Any:
        """Get an attribute."""
        return getattr(self, key)

    def __init__(self, hard_coded_value: Any = all_default_attrs["hard_coded_value"], 
                **kwargs):
        if self._initialized:
            return
        self.hard_coded_value = hard_coded_value
        super().__init__(**kwargs)        