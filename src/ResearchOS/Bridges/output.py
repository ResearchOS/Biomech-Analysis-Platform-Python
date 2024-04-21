from typing import TYPE_CHECKING, Union, Any
import json

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.action import Action
    from ResearchOS.research_object import ResearchObject
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.put import Put

class Output(Put):
    """Output Put to connect between DiGraphs."""

    is_input: bool = False

    def __init__(self, id: int = None,
                 vr: "Variable" = None,
                 pr: "source_type" = None,
                 show: bool = False,
                 action: "Action" = None):
        """Initializes the Output object. "vr" and "pr" together make up the main source of the output."""
        self.is_input = False
        if id is None:
            self.clean_for_put(vr = vr, pr = pr, show = show, action = action)
        super().__init__(id=id, 
                         action=action, 
                         dynamic_vr_id=self.dynamic_vr_id, 
                         is_input=self.is_input, 
                         order_num=self.order_num,
                         is_lookup=self.is_lookup,
                         value=self.value)
