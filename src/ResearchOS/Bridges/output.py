from typing import TYPE_CHECKING, Union, Any
import json

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.action import Action
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.put import Put
from ResearchOS.Bridges.input_types import Dynamic

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
        dynamic_vr = [Dynamic(vr=vr, pr=pr, is_input=False, action=action)]
        super().__init__(id = id, action = action, dynamic_vrs=dynamic_vr)        

        # self.vr = vr
        # self.pr = pr
        # self.show = show 
        # self.clean_for_put(vr = vr, pr = pr, show = show, action = action)
        
        # # Make sure the dynamic VR is created.
        # dynamic_vrs = []
        # if not isinstance(pr, list):
        #     pr = [pr]
        # dynamic_vrs = self.dynamic_vrs
        # if not dynamic_vrs:
        #     dynamic_vr_ids = [None]
        # else:
        #     dynamic_vr_ids = [d.id if d is not None else None for d in dynamic_vrs]
        # is_input = [False]
        # order_num = [0]
        # is_lookup = [False]     
        # super().__init__(id=id, 
        #                  action=action, 
        #                  dynamic_vrs=dynamic_vrs,
        #                  is_input=is_input,
        #                  order_num=order_num,
        #                  is_lookup=is_lookup,
        #                  dynamic_vr_id=dynamic_vr_ids)
