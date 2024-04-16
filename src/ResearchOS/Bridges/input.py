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
import ResearchOS.Bridges.input_types as it

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
        
        # 1. import file vr name
        import_value_default = "import_file_vr_name"
        if let is not None and hasattr(let, "parent_ro") and let.vr_name_in_code==let.parent_ro.import_file_vr_name:
            input = it.ImportFile(import_value_default)
        elif isinstance(value, dict):
            key = list(value.keys())[0]
            if key.__class__ == type:
                input = it.DataObjAttr(key, value[key])
            else:
                input = it.HardCoded(value)
        elif value is not None:
            # 2. hard-coded value.
            input = it.HardCoded(value)
        elif vr is not None:
            # 3. dynamic value.
            input = it.DynamicMain(it.Dynamic(vr=vr, pr=pr), it.Dynamic(vr=lookup_vr, pr=lookup_pr), show=show)
        else:
            input = self

        self.put_value = input
        self.action = action
        self.let = let

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