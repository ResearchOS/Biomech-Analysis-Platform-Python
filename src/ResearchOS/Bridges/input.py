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
    def set_source_pr(parent_ro: "ResearchObject", vr: "Variable", G: nx.MultiDiGraph = None) -> "source_type":
        """Set the source process or logsheet."""
        from ResearchOS.research_object_handler import ResearchObjectHandler
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet

        try:
            anc_nodes = list(nx.ancestors(G, parent_ro))
        except:
            anc_nodes = [n for n in G.nodes]
        subgraph = G.subgraph(anc_nodes)
        nodes_sorted = list(reversed(list(nx.topological_sort(subgraph)))) # Latest first.
        if parent_ro in nodes_sorted:
            nodes_sorted.remove(parent_ro)

        for node in nodes_sorted:
            if isinstance(node, Logsheet):
                for h in node.headers:
                    vr = vr.id if isinstance(vr, Variable) else vr
                    if h[3].id == vr:
                        try:
                            return node
                        except:
                            pass                        
                continue
            for output in node.outputs.values():
                output_main_vr = output["main"]["vr"]
                output_lookup_vr = output["lookup"]["vr"]
                if output_main_vr == vr.id or output_lookup_vr == vr.id:
                    try:
                        return node
                    except:
                        pass