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
        self.init_helper(id=id, 
                         vr=vr, 
                         pr=pr, 
                         lookup_vr=lookup_vr, 
                         lookup_pr=lookup_pr, 
                         value=value, 
                         show=show, 
                         action=action, 
                         parent_ro=parent_ro, 
                         vr_name_in_code=vr_name_in_code, 
                         **kwargs)        
        self.is_input = True
        super().__init__()

    def set_source_pr(parent_ro: "ResearchObject", vr: "Variable", vr_name_in_code: str = None) -> "source_type":
        """Set the source process or logsheet."""
        from ResearchOS.research_object_handler import ResearchObjectHandler
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        
        prs = [pr() for pr in ResearchObjectHandler.instances_list if pr() is not None and isinstance(pr(), (Process,))]
        lgs = [lg() for lg in ResearchObjectHandler.instances_list if lg() is not None and isinstance(lg(), (Logsheet,))]

        if isinstance(parent_ro, Process):
            last_idx = prs.index(parent_ro)
        else:
            last_idx = len(prs)
        prs = prs[0:last_idx] # Reverse the list to get the most recent PRs first (go backward in time)
        prs.reverse()

        final_pr = None        
        for pr in prs:
            outputs = pr.outputs.values()
            for output in outputs:
                if output is not None and output.vr == vr:                    
                    final_pr = pr
                    break # Found the proper pr.
            if final_pr:
                break

        if not final_pr:
            for lg in lgs:
                for h in lg.headers:
                    if h[3] == vr:
                        final_pr = lg
                        break

        if not final_pr and hasattr(parent_ro, "import_file_vr_name") and vr_name_in_code==parent_ro.import_file_vr_name:
            final_pr = parent_ro

        if not final_pr:
            raise ValueError(f"Could not find the source PR for VR {vr}. If this is an import_file_vr, then put that attribute before the 'set_inputs' command.")

        return final_pr