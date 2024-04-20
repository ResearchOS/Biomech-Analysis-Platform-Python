from typing import TYPE_CHECKING, Union, Any
import json

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.action import Action
    from ResearchOS.research_object import ResearchObject
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.port import Port

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
                 value: Any = None,
                 lookup_vr: "Variable" = None,
                 lookup_pr: "source_type" = None,    
                 **kwargs):
        """Initializes the Output object. "vr" and "pr" together make up the main source of the output."""

        none_list = [value, lookup_vr, lookup_pr]
        if not all([x is None for x in none_list]):
            raise ValueError("Provided one of these illegal kwargs to outputs: ", none_list)

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
        self.is_input = False
        super().__init__()
