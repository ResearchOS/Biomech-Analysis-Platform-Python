from typing import Any

from ResearchOS.research_object import ResearchObject

all_default_attrs = {}

computer_specific_attr_names = []

class Variable(ResearchObject):
    """Variable class."""

    prefix: str = "VR"
    _initialized: bool = False

    def __getitem__(self, slice: tuple) -> Any:
        """Store the slice of the Variable."""
        self.slice = slice
        return self

    def __init__(self, 
                **kwargs):
        if self._initialized:
            return      
        super().__init__(**kwargs)