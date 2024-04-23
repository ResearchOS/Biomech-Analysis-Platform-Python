from typing import TYPE_CHECKING, Union, Any

import networkx as nx

if TYPE_CHECKING:      
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

from ResearchOS.variable import Variable  

class Input():
    """Input Put to connect between DiGraphs.""" 

    def __init__(self,
                 vr: "Variable" = None, 
                 pr: "source_type" = None,
                 lookup_vr: "Variable" = None,
                 lookup_pr: "source_type" = None,
                 value: Any = None,
                 show: bool = False):
        """Initializes the Input object. "vr" and "pr" together make up the main source of the input. "lookup_vr" and "lookup_pr" together make up the lookup source of the input.
        "value" is the hard-coded value. If specified, supercedes the main source."""
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        # Turn them into lists if not None.
        pr = [pr] if pr and not isinstance(pr, list) else pr
        lookup_pr = [lookup_pr] if lookup_pr and not isinstance(lookup_pr, list) else lookup_pr     

        # Ensure that they are ID's (or None) not objects.
        vr_id = vr.id if isinstance(vr, Variable) else vr
        pr_id = [p.id if isinstance(p, (Logsheet, Process)) else p for p in pr] if pr else []
        lookup_vr_id = lookup_vr.id if isinstance(lookup_vr, Variable) else lookup_vr
        lookup_pr_id = [p.id if isinstance(p, (Logsheet, Process)) else p for p in lookup_pr] if lookup_pr else []

        # Store the values.
        self.value = value
        self.show = show
        self.main = {}
        self.lookup = {}
        self.main["vr"] = vr_id
        self.main["pr"] = pr_id
        self.lookup["vr"] = lookup_vr_id
        self.lookup["pr"] = lookup_pr_id

    @staticmethod
    def set_source_pr(parent_ro: "ResearchObject", vr: "Variable", vr_name_in_code: str = None, G: nx.MultiDiGraph = None) -> "source_type":
        """Set the source process or logsheet."""
        from ResearchOS.research_object_handler import ResearchObjectHandler
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet

        nodes_sorted = list(reversed(list(nx.topological_sort(G)))) # Latest first.
        if parent_ro in nodes_sorted:
            nodes_sorted.remove(parent_ro)

        for node in nodes_sorted:
            if isinstance(node, Logsheet):
                for h in node.headers:
                    vr = vr.id if isinstance(vr, Variable) else vr
                    if h[3].id == vr:
                        return node
                continue
            for output in node.outputs.values():
                output_main_vr = output["main"]["vr"]
                output_lookup_vr = output["lookup"]["vr"]
                if output_main_vr == vr.id or output_lookup_vr == vr.id:
                    return node                        

        
        # prs = [pr() for pr in ResearchObjectHandler.instances_list if pr() is not None and isinstance(pr(), (Process,))]
        # lgs = [lg() for lg in ResearchObjectHandler.instances_list if lg() is not None and isinstance(lg(), (Logsheet,))]

        # if isinstance(parent_ro, Process):
        #     last_idx = prs.index(parent_ro)
        # else:
        #     last_idx = len(prs)
        # prs = prs[0:last_idx] # Reverse the list to get the most recent PRs first (go backward in time)
        # prs.reverse()

        # final_pr = None        
        # for pr in prs:
        #     outputs = pr.outputs
        #     for vr_name, output_vr in outputs.items():
        #         output_main_vr = output_vr["main"]["vr"]
        #         vr_id = vr.id if isinstance(vr, Variable) else vr
        #         if output_main_vr is not None and output_main_vr == vr_id:                    
        #             final_pr = pr
        #             break # Found the proper pr.
        #         output_lookup_vr = output_vr["lookup"]["vr"]
        #         if output_lookup_vr is not None and output_lookup_vr == vr_id:
        #             final_pr = pr
        #             break
        #     if final_pr:
        #         break

        # if not final_pr:
        #     for lg in lgs:
        #         for h in lg.headers:
        #             vr = vr.id if isinstance(vr, Variable) else vr
        #             if h[3].id == vr:
        #                 final_pr = lg
        #                 break

        # if not final_pr and not (hasattr(parent_ro, "import_file_vr_name") and vr_name_in_code==parent_ro.import_file_vr_name):            
        #     raise ValueError(f"Could not find the source PR for VR {vr}. If this is an import_file_vr, then put that attribute before the 'set_inputs' command.")

        # return final_pr