from typing import TYPE_CHECKING, Union
import json

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.action import Action
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.port import Port
import ResearchOS.Bridges.input_types as it
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.Bridges.input_types import Dynamic

class Output(Port):
    """Output port to connect between DiGraphs."""

    is_input: bool = False

    def __init__(self, id: int = None,
                 vr: "Variable" = None,
                 pr: "source_type" = None,
                 show: bool = False,
                 action: "Action" = None,
                 parent_ro: "ResearchObject" = None,
                 vr_name_in_code: str = None,
                 **kwargs):
        """Initializes the Output object. "vr" and "pr" together make up the main source of the output."""

        if hasattr(self, "id"):
            return
        
        if id is not None:
            # Run the SQL query to load the values.
            sqlquery_raw = "SELECT id, is_input, value, show, ro_id, vr_name_in_code FROM inputs_outputs WHERE id = ?"
            sqlquery = sql_order_result(action, sqlquery_raw, ["id"], single=True, user = True, computer = False)
            params = (id,)
            result = action.conn.execute(sqlquery, params).fetchall()
            if not result:
                raise ValueError(f"Port with id {id} not found in database.")
            id, is_input, value, show, ro_id, vr_name_in_code = result[0]
            value = json.loads(value)
            sqlquery_raw = "SELECT dynamic_vr_id FROM inputs_outputs_to_dynamic_vrs WHERE io_id = ?"
            sqlquery = sql_order_result(action, sqlquery_raw, ["id"], single=False, user = True, computer = False)
            params = (id,)
            result = action.conn.execute(sqlquery, params).fetchall()
            if result:
                main_dynamic_vr = Dynamic(id=result[0][0], action=action)            
            vr = main_dynamic_vr.vr if result is not None else None
            pr = main_dynamic_vr.pr if result is not None else None

        # Now same parameter values whether loading or creating new.
        if vr is None and pr is None:
            output = it.NoneVR()
        else:
            output = it.DynamicMain(it.Dynamic(vr=vr, pr=pr, action=action), show=show)
        
        self.put_value = output
        self.action = action
        self.parent_ro = parent_ro
        self._id = id
        self.vr_name_in_code = vr_name_in_code
        self.return_conn = False # Because I never intend to call Output() directly, without an Action provided.

        super().__init__()
