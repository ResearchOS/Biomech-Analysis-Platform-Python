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
        from ResearchOS.variable import Variable
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet

        if hasattr(self, "id"):
            return
        
        if id is not None:
            # Run the SQL query to load the values.
            sqlquery_raw = "SELECT id, is_input, vr_id, pr_id, lookup_vr_id, lookup_pr_id, value, show, ro_id, vr_name_in_code FROM inputs_outputs WHERE id = ?"
            sqlquery = sql_order_result(action, sqlquery_raw, ["id"], single=True, user = True, computer = False)
            params = (id,)
            result = action.conn.execute(sqlquery, params).fetchall()
            if not result:
                raise ValueError(f"Port with id {id} not found in database.")
            id, is_input, vr_id, pr_id, lookup_vr_id, lookup_pr_id, value, show, ro_id, vr_name_in_code = result[0]
            if not pr_id:
                pr_id = json.dumps([])
            value = json.loads(value)
            vr = Variable(id=vr_id, action=action) if vr_id is not None else None
            pr = []
            for p in json.loads(pr_id):
                if p is None:
                    pr.append(None)
                else:
                    if p.startswith("PR"):
                        pr.append(Process(id=p, action=action))
                    elif p.startswith("LG"):
                        pr.append(Logsheet(id=p, action=action))
            if len(pr)==1:
                pr = pr[0]

        # Now same parameter values whether loading or creating new.
        if vr is None and pr is None:
            output = it.NoneVR(show = True)
        else:
            output = it.DynamicMain(it.Dynamic(vr=vr, pr=pr), show=show)
        
        self.put_value = output
        self.action = action
        self.parent_ro = parent_ro
        self._id = id
        self.vr_name_in_code = vr_name_in_code
        self.return_conn = False # Because I never intend to call Output() directly, without an Action provided.

        super().__init__()
