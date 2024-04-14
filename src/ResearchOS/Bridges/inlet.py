from ResearchOS.Bridges.inlet_or_outlet import InletOrOutlet
from ResearchOS.action import Action

class Inlet(InletOrOutlet):
    """Represents a place where an edge can be connected to a PR as an input."""

    is_inlet: bool = True
    attr_name: str = "input"

    def __init__(self, parent_ro, vr_name_in_code, action: Action = None):
        """Initializes the Inlet object."""        
        super().__init__(parent_ro, vr_name_in_code, action)            

        