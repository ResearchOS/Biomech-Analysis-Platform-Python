from typing import TYPE_CHECKING, Union, Any
import weakref, json

import networkx as nx

if TYPE_CHECKING:
    from ResearchOS.variable import Variable    
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    from ResearchOS.research_object import ResearchObject
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.put import Put
from ResearchOS.action import Action

class Input(Put):
    """Input Put to connect between DiGraphs."""

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