from ResearchOS.Bridges.inlet_or_outlet import InletOrOutlet

class Inlet(InletOrOutlet):
    """Represents a place where an edge can be connected to a PR as an input."""

    is_inlet: bool = True
    attr_name: str = "input"

    def __init__(self, parent_ro, vr_name_in_code, action=None):
        """Initializes the Inlet object."""
        self.input = None        
        super().__init__(parent_ro, vr_name_in_code, action)
        

        
        