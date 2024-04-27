from typing import Any

from ResearchOS.research_object import ResearchObject

all_default_attrs = {}
all_default_attrs["hard_coded_value"] = None

computer_specific_attr_names = []

class Variable(ResearchObject):
    """Variable class."""

    prefix: str = "VR"
    _initialized: bool = False

    def __getitem__(self, slice_input: tuple) -> Any:
        """Store the slice of the Variable."""
        slice_list = self.slice_to_list(slice_input)
        self._slice = slice_list
        return self

    def __init__(self, 
                    hard_coded_value: Any = all_default_attrs["hard_coded_value"],
                    **kwargs):
        if self._initialized:
            return
        self.hard_coded_value = hard_coded_value
        self._slice = None
        super().__init__(**kwargs)

    def slice_to_list(self, slice_input: tuple) -> list:
        """Return the slice as a list."""
        slice_list = []
        for s in slice_input:
            if isinstance(s, slice):
                start = s.start if s.start is not None else "None"
                stop = s.stop if s.stop is not None else "None"
                step = s.step if s.step is not None else "None"
                s = [start, stop, step]
            slice_list.append(s)
        return slice_list