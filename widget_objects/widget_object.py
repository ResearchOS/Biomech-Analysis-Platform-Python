

from typing import Any


class WidgetObject():
    """Parent class for visualizing all research objects."""
    def __init__():
        pass

    def visualize():
        raise NotImplementedError
    
    def __setattr__(self, __name: str, __value: Any) -> None:
        raise NotImplementedError