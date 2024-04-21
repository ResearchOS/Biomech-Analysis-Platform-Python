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
        
        self.vr = vr
        self.pr = pr
        self.lookup_vr = lookup_vr
        self.lookup_pr = lookup_pr
        self.value = value
        self.show = show 
        # Convert this input to those suitable for a Put object:
        if id is not None:
            self.clean_for_put(vr=vr, pr=pr, lookup_vr=lookup_vr, lookup_pr=lookup_pr, value=value, show=show, action=action)
        
        # Make sure the main PR is properly initialized.
        main_vrs = []
        if (vr is None and pr is None) or (vr is not None and pr is not None):
            do_init = True
            if not isinstance(pr, list):
                pr = [pr]
            main_vrs = [Dynamic(vr=vr, pr=pr, order_num=idx, action=action) for idx, pr in enumerate(pr)]
        else:
            do_init = False

        # Make sure the lookup PR is properly initialized.
        lookup_vrs = []
        if do_init and ((lookup_vr is None and lookup_pr is None) or (lookup_vr is not None and lookup_pr is not None)):
            do_init = True
            if lookup_vr is not None: # Within this if statement because don't want to replicate the None Dynamic VR potentially created above.
                if not isinstance(lookup_pr, list):
                    lookup_pr = [lookup_pr]
                lookup_vrs = [Dynamic(vr=lookup_vr, pr=lookup_pr, order_num=idx, is_lookup=True, action=action) for idx, lookup_pr in enumerate(lookup_pr)]
        else:
            do_init = False
        if do_init:
            dynamic_vrs=main_vrs+lookup_vrs
            super().__init__(id=id, action=action, dynamic_vrs=dynamic_vrs)