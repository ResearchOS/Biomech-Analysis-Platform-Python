from ResearchOS.Bridges.inlet_or_outlet import InletOrOutlet

class Inlet(InletOrOutlet):
    """Represents a place where an edge can be connected to a PR as an input."""

    table_name: str = "inlets"
    id_col: str = "inlet_id"
        

        
        