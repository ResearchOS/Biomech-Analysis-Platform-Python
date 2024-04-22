from typing import TYPE_CHECKING, Union, Any

if TYPE_CHECKING:
    from ResearchOS.variable import Variable    
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

from ResearchOS.Bridges.put import Put
from ResearchOS.action import Action
from ResearchOS.Bridges.input_types import Dynamic


class Input(Put):
    """Input Put to connect between DiGraphs.""" 

    def __init__(self, id: int = None,
                 vr: "Variable" = None, 
                 pr: "source_type" = None,
                 lookup_vr: "Variable" = None,
                 lookup_pr: "source_type" = None,
                 value: Any = None,
                 show: bool = False,
                 action: "Action" = None,
                 **kwargs):
        """Initializes the Input object. "vr" and "pr" together make up the main source of the input. "lookup_vr" and "lookup_pr" together make up the lookup source of the input.
        "value" is the hard-coded value. If specified, supercedes the main source.""" 
        self.is_input = True
        self.value = value
        self.show = show
        main_dynamic_vr = [Dynamic(vr=vr, pr=pr, action=action) for pr in pr] if pr is not None else None
        lookup_dynamic_vr = [Dynamic(vr=lookup_vr, pr=lookup_pr, is_lookup=True, action=action) for lookup_pr in lookup_pr] if lookup_pr is not None else None
        if main_dynamic_vr is None:
            main_dynamic_vr = []
        if lookup_dynamic_vr is None:
            lookup_dynamic_vr = []        
        dynamic_vrs = main_dynamic_vr + lookup_dynamic_vr
        if not dynamic_vrs:
            dynamic_vrs = None        
        super().__init__(id=id, action=action,
                         dynamic_vrs=dynamic_vrs)
        # self.vr = vr
        # self.pr = pr
        # self.lookup_vr = lookup_vr
        # self.lookup_pr = lookup_pr
        # self.value = value
        # self.show = show 
        # # Convert this input to those suitable for a Put object:
        # self.clean_for_put(vr=vr, pr=pr, lookup_vr=lookup_vr, lookup_pr=lookup_pr, value=value, show=show, action=action)
        
        # # Make sure the main PR is properly initialized.
        # main_vrs = []
        # do_init = False
        # if (vr is None and pr is None) or (vr is not None and pr is not None):
        #     do_init = True        

        # # Make sure the lookup PR is properly initialized.
        # lookup_vrs = []
        # if do_init and ((lookup_vr is None and lookup_pr is None) or (lookup_vr is not None and lookup_pr is not None)):
        #     do_init = True
        # else:
        #     do_init = False
                
        # if do_init:
        #     dynamic_vrs=self.dynamic_vrs+self.lookup_vrs
        #     is_input = [True]
        #     order_num = [0]
        #     is_lookup = [False]
        #     dynamic_vr_id = [d.id for d in dynamic_vrs]
        #     if dynamic_vrs:
        #         is_input = [True]*len(dynamic_vrs)
        #         order_num = [d.order_num for d in dynamic_vrs]
        #         is_lookup = [d.is_lookup for d in dynamic_vrs]
        #     else:
        #         dynamic_vrs = [None]
        #         dynamic_vr_id = [None]
        #     super().__init__(id=id, action=action, 
        #                      dynamic_vrs=dynamic_vrs, 
        #                      dynamic_vr_id=dynamic_vr_id,
        #                      is_input=is_input, 
        #                      order_num=order_num, 
        #                      is_lookup=is_lookup)