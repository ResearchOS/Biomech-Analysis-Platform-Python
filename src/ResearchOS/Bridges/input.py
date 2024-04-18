from typing import TYPE_CHECKING, Union, Any
import weakref, json

import networkx as nx

if TYPE_CHECKING:
    from ResearchOS.variable import Variable    
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    from ResearchOS.research_object import ResearchObject
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.port import Port
from ResearchOS.sql.sql_runner import sql_order_result
import ResearchOS.Bridges.input_types as it
from ResearchOS.action import Action

class Input(Port):
    """Input port to connect between DiGraphs."""

    is_input: bool = True

    def __eq__(self, other: "Input") -> bool:
        return self.id == other.id

    def __init__(self, id: int = None,
                 vr: "Variable" = None, 
                 pr: "source_type" = None,
                 lookup_vr: "Variable" = None,
                 lookup_pr: "source_type" = None,
                 value: Any = None,
                 show: bool = False,
                 action: "Action" = None,
                 parent_ro: "ResearchObject" = None,
                 vr_name_in_code: str = None,
                 **kwargs):
        """Initializes the Input object. "vr" and "pr" together make up the main source of the input. "lookup_vr" and "lookup_pr" together make up the lookup source of the input.
        "value" is the hard-coded value. If specified, supercedes the main source."""
        from ResearchOS.variable import Variable
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet

        return_conn = False
        if action is None:
            action = Action(name = "Set_Inputs")
            return_conn = True

        if hasattr(self, "id"):
            return # Already initialized, loaded from Port.instances

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
                    pr = None
                    break
                if p.startswith("PR"):
                    pr.append(Process(id=p, action=action))
                elif p.startswith("LG"):
                    pr.append(Logsheet(id=p, action=action))
            if len(pr)==1:
                pr = pr[0]
            elif len(pr)==0:
                pr = None
            lookup_vr = Variable(id=lookup_vr_id, action=action) if lookup_vr_id is not None else None
            lookup_pr = Process(id=lookup_pr_id, action=action) if lookup_pr_id is not None else None
            is_input = bool(is_input)
            show = bool(show)
        
        # Now whether loading or saving, all inputs are properly shaped.
        if isinstance(value, dict) and (vr is None or vr.hard_coded_value is None):
            key = list(value.keys())[0]
            if key.__class__ == type:
                input = it.DataObjAttr(key, value[key])
            else:
                input = it.HardCoded(value)
        elif hasattr(parent_ro, "import_file_vr_name") and vr_name_in_code==parent_ro.import_file_vr_name:
            import_value_default = "import_file_vr_name"   
            input = it.ImportFile(import_value_default)
        elif vr is not None and vr.hard_coded_value is not None:
            input = it.HardCodedVR(vr=vr, value=vr.hard_coded_value)
        elif value is not None:
            # 2. hard-coded value.
            input = it.HardCoded(value)        
        elif vr is not None:
            # 3. dynamic value. Also the import_file_vr_name would fit here because it's a VR, but that gets overwritten in the VRHandler.            
            input = it.DynamicMain(it.Dynamic(vr=vr, pr=pr), it.Dynamic(vr=lookup_vr, pr=lookup_pr), show=show)
        else:
            input = it.NoneVR()

        self.vr_name_in_code = vr_name_in_code
        self.put_value = input
        self.action = action
        self.parent_ro = parent_ro
        self._id = id
        self.return_conn = return_conn
        super().__init__()

    def set_source_pr(parent_ro: "ResearchObject", vr: "Variable"):
        """Set the source process or logsheet."""
        from ResearchOS.research_object_handler import ResearchObjectHandler
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        
        prs = [pr() for pr in ResearchObjectHandler.instances_list if pr() is not None and isinstance(pr(), (Process,))]
        lgs = [lg() for lg in ResearchObjectHandler.instances_list if lg() is not None and isinstance(lg(), (Logsheet,))]

        last_idx = prs.index(parent_ro)
        prs = prs[0:last_idx] # Reverse the list to get the most recent PRs first (go backward in time)
        prs.reverse()

        final_pr = None        
        for pr in prs:
            outputs = pr.outputs.values()
            for output in outputs:
                if output is not None and output.vr == vr:                    
                    final_pr = pr
                    break # Found the proper pr.

        if not final_pr:
            for lg in lgs:
                for h in lg.headers:
                    if h[3] == vr:
                        final_pr = lg
                        break

        if not final_pr:
            raise ValueError(f"Could not find the source PR for VR {vr}. If this is an import_file_vr, then put that attribute before the 'set_inputs' command.")

        return final_pr