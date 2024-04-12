from ResearchOS.Bridges.inlet_or_outlet import InletOrOutlet

class Outlet(InletOrOutlet):
    """Represents a place where an edge can be connected to a PR as an input."""

    table_name: str = "outlets"
    id_col: str = "outlet_id"