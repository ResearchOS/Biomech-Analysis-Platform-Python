from typing import TYPE_CHECKING, Union, Any
import weakref

import networkx as nx

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.action import Action
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    from ResearchOS.Bridges.inlet_or_outlet import InletOrOutlet
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.port import Port
from ResearchOS.Bridges.output import Output
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.Bridges.edge import Edge

class Input(Port):
    """Input port to connect between DiGraphs."""

    is_input: bool = True

    def __init__(self, id: int = None,
                 vr: "Variable" = None, 
                 pr: "source_type" = None,
                 lookup_vr: "Variable" = None,
                 lookup_pr: "source_type" = None,
                 value: Any = None,
                 show: bool = False,
                 src: "Output" = None,
                 action: "Action" = None,
                 let: "InletOrOutlet" = None):
        """Initializes the Input object. "vr" and "pr" together make up the main source of the input. "lookup_vr" and "lookup_pr" together make up the lookup source of the input.
        "value" is the hard-coded value. If specified, supercedes the main source."""              

        if src is not None:
            vr = src.vr
            pr = src.pr        
        
        self.action=action        
        if pr is None and vr is None:
            show = True            
        # Make sure this is a truly dynamic variable that needs an edge from an Output to have a value.
        is_hard_coded = (value is not None and vr is None) or vr.hard_coded_value is not None
        if pr is None and not is_hard_coded:
            if let is None:
                raise ValueError("If specifying Input and a dynamic VR, then must also specify the source PR. Alternatively, just specify the VR and let the system find the most recent source PR.")
            is_hard_coded = vr.hard_coded_value is not None or (hasattr(let.parent_ro, "import_file_vr_name") and let.parent_ro.import_file_vr_name == let.vr_name_in_code)
            if vr is not None and vr.hard_coded_value is not None:
                value = vr.hard_coded_value
                vr = None
        if not is_hard_coded and pr is None:
            if let is None:
                raise ValueError("If specifying Input and a dynamic VR, then must also specify the source PR. Alternatively, just specify the VR and let the system find the most recent source PR.")           
            self.add_attrs(let.parent_ro, let.vr_name_in_code)
            pr = self.set_source_pr(vr)
            assert pr is not None
        self.lookup_vr = lookup_vr
        self.lookup_pr = lookup_pr
        self.value = value
        super().__init__(id=id, vr=vr, pr=pr, show=show, action=action)

    def set_source_pr(self, vr: "Variable"):
        """Set the source process or logsheet."""
        from ResearchOS.research_object_handler import ResearchObjectHandler
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        
        prs = [pr() for pr in ResearchObjectHandler.instances_list if pr() is not None and isinstance(pr(), (Process,))]
        lgs = [lg() for lg in ResearchObjectHandler.instances_list if lg() is not None and isinstance(lg(), (Logsheet,))]

        last_idx = prs.index(self.parent_ro)
        prs = prs[0:last_idx] # Reverse the list to get the most recent PRs first (go backward in time)
        prs.reverse()

        final_pr = None        
        for pr in prs:
            outlets = pr.outputs.values()
            for outlet in outlets:
                output = outlet.puts[0]
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