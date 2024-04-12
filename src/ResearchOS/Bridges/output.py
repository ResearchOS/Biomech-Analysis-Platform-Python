from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]
    from ResearchOS.Bridges.input import Input

from ResearchOS.Bridges.port import Port
from ResearchOS.Bridges.edge import Edge
from ResearchOS.action import Action
from ResearchOS.sqlite_pool import SQLiteConnectionPool

class Output(Port):
    """Output port to connect between DiGraphs."""

    def __init__(self, vr: "Variable" = None, vr_name_in_code: str = None, show: bool = False):
        """Initializes the Input object"""
        self.vr = vr
        if not vr:
            show = True
        self.show = show
        self.vr_name_in_code = vr_name_in_code

    def connect_to(self, input: "Input", action: Action) -> None:
        """Connects the Output to the inlet of another Process."""
        self.input = input
        self.target_pr = input.parent_ro
        self.target_vr = input.vr

        return_conn = False
        if action is None:
            pool = SQLiteConnectionPool()
            return_conn = True
            action = Action(name = "create_edge")

        # 1. Create a new Edge object.
        edge = Edge(self, input, action=action)

        # 2. Put the Edge object in the pipelineobjects_graph.
        sqlquery = "INSERT INTO pipelineobjects_graph (action_id_num, source_object_id, target_object_id, edge_id) VALUES (?, ?, ?, ?)"
        params = (self.action.id, self.source_pr.id, self.target_pr.id, edge.id)
        action.conn.execute(sqlquery, params)

        if return_conn:
            pool.return_connection(action.conn)
