from typing import TYPE_CHECKING, Union, Any

if TYPE_CHECKING:
    from ResearchOS.variable import Variable    
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]


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
        # Turn them into lists if not None.
        pr = [pr] if pr and not isinstance(pr, list) else pr
        lookup_pr = [lookup_pr] if lookup_pr and not isinstance(lookup_pr, list) else lookup_pr     

        # Ensure that they are ID's (or None) not objects.
        vr_id = vr.id if isinstance(vr, Variable) else vr
        pr_id = [p.id if isinstance(p, source_type) else p for p in pr]
        lookup_vr_id = lookup_vr.id if isinstance(lookup_vr, Variable) else lookup_vr
        lookup_pr_id = [p.id if isinstance(p, source_type) else p for p in lookup_pr]

        # Store the values.
        self.value = value
        self.show = show
        self.main.vr = vr_id
        self.main.pr = pr_id
        self.lookup.vr = lookup_vr_id
        self.lookup.pr = lookup_pr_id