from ResearchOS.Bridges.inlet_or_outlet import InletOrOutlet
from ResearchOS.Bridges.inlet import Inlet
from ResearchOS.action import Action
from ResearchOS.sqlite_pool import SQLiteConnectionPool

class Outlet(InletOrOutlet):
    """Represents a place where an edge can be connected to a PR as an input."""
    
    is_inlet: bool = False
    attr_name: str = "output"

    def __init__(self, parent_ro, vr_name_in_code, action: Action = None):
        """Initializes the Outlet object."""
        self.output = None
        super().__init__(parent_ro, vr_name_in_code, action)

    def connect_to(self, inlet: Inlet, action: Action) -> None:
        """Connects the Output to the inlet of another Process."""
        from ResearchOS.Bridges.edge import Edge

        return_conn = False
        if action is None:
            pool = SQLiteConnectionPool()
            return_conn = True
            action = Action(name = "create_edge")

        # 1. Create a new Edge object.
        edge = Edge(self, inlet, action=action)

        # 2. Put the Edge object in the pipelineobjects_graph.
        query_name = "insert_pl_edge"
        sqlquery = action.queries[query_name]
        params = (self.action.id, edge.src.parent_ro.id, edge.dest.parent_ro.id, edge.id)
        
        if return_conn:
            action.conn.execute(sqlquery, params)
            pool.return_connection(action.conn)
        else:
            action.add_sql_query(None, query_name, params)