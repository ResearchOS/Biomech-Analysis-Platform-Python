from typing import TYPE_CHECKING, Union, Any
import json
import logging

if TYPE_CHECKING:
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    from ResearchOS.variable import Variable
    from ResearchOS.research_object import ResearchObject
    source_type = Union[Process, Logsheet]

from ResearchOS.action import Action
from ResearchOS.Bridges.input_types import Dynamic
from ResearchOS.Bridges.pipeline_parts import PipelineParts

logger = logging.getLogger("ResearchOS")

class Put(PipelineParts):
    """Base class for the various Put mechanisms to connect between DiGraphs."""

    cls_name = "Put"
    table_name = "inputs_outputs"
    id_col = "put_id"
    col_names = ["dynamic_vr_id", "is_input", "order_num", "is_lookup"]
    insert_query_name = "inputs_outputs_insert"
    init_attr_names = ["dynamic_vrs", "is_input", "order_num", "is_lookup"]
    allowable_none_cols = ["dynamic_vr_id"]

    def __init__(self, id: int = None,
                 dynamic_vr_id: list = [None],
                 is_input: list = [True],
                 order_num: list = [0],
                 is_lookup: list = [False],
                 dynamic_vrs: list = [None],
                 action: Action = None):
        """Initializes the Put object."""
        if dynamic_vrs and all([d is not None for d in dynamic_vrs]):
            dynamic_vr_id = []
            is_input = []
            order_num = []
            is_lookup = []
        for dynamic_vr in dynamic_vrs:
            if not dynamic_vr:
                continue
            is_lookup.append(dynamic_vr.is_lookup)
            dynamic_vr_id.append(dynamic_vr.id)
            is_input.append(dynamic_vr.is_input)
            order_num.append(dynamic_vr.order_num)
        # Use defaults if any of the dynamic_vr_id is None.
        if any([d is None for d in dynamic_vr_id]) and not all([d is None for d in dynamic_vr_id]):
            raise ValueError(f"Why?")
        self.dynamic_vrs = dynamic_vrs
        self.dynamic_vr_id = dynamic_vr_id
        self.is_input = is_input
        self.order_num = order_num
        self.is_lookup = is_lookup
        where_str = ""
        params = []
        for idx in range(max(len(dynamic_vr_id), 1)):
            curr_str = "(dynamic_vr_id = ? AND is_input = ? AND order_num = ? AND is_lookup = ?)"
            if idx == 0:
                where_str += curr_str
            else:
                where_str += " OR " + curr_str
            if dynamic_vr_id:
                params += [dynamic_vr_id[idx], int(is_input[idx]), order_num[idx], int(is_lookup[idx])]
        if params:
            self.params = tuple(params)
            self.input_args = [dynamic_vr_id, is_input, order_num, is_lookup]
        self.where_str = where_str
        super().__init__(id = id, action = action)

    def load_from_db(self, dynamic_vr_id: list = [],
                     is_input: list = [],
                     order_num: list = [],
                     is_lookup: list = [],
                     action: Action = None):
        """Load the dynamic_vr object from the database."""
        if not isinstance(dynamic_vr_id, list):
            dynamic_vr_id = [dynamic_vr_id]
        if not isinstance(is_input, list):
            is_input = [is_input]
        if not isinstance(order_num, list):
            order_num = [order_num]
        if not isinstance(is_lookup, list):
            is_lookup = [is_lookup]

        dynamic_vrs = [Dynamic(id = dynamic_vr_id[idx], action = action) for idx in range(len(dynamic_vr_id))]
        self.dynamic_vrs = dynamic_vrs
        self.is_input = [d.is_input for d in dynamic_vrs]
        self.order_num = [d.order_num for d in dynamic_vrs]
        self.is_lookup = [d.is_lookup for d in dynamic_vrs]

    def set_source_pr(parent_ro: "ResearchObject", vr: "Variable", vr_name_in_code: str = None) -> "source_type":
        """Set the source process or logsheet."""
        from ResearchOS.research_object_handler import ResearchObjectHandler
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        
        prs = [pr() for pr in ResearchObjectHandler.instances_list if pr() is not None and isinstance(pr(), (Process,))]
        lgs = [lg() for lg in ResearchObjectHandler.instances_list if lg() is not None and isinstance(lg(), (Logsheet,))]

        if isinstance(parent_ro, Process):
            last_idx = prs.index(parent_ro)
        else:
            last_idx = len(prs)
        prs = prs[0:last_idx] # Reverse the list to get the most recent PRs first (go backward in time)
        prs.reverse()

        final_pr = None        
        for pr in prs:
            outputs = pr.outputs.values()
            for output in outputs:
                if output is not None and output.vr == vr:                    
                    final_pr = pr
                    break # Found the proper pr.
            if final_pr:
                break

        if not final_pr:
            for lg in lgs:
                for h in lg.headers:
                    if h[3] == vr:
                        final_pr = lg
                        break

        if not final_pr and hasattr(parent_ro, "import_file_vr_name") and vr_name_in_code==parent_ro.import_file_vr_name:
            final_pr = parent_ro

        if not final_pr:
            raise ValueError(f"Could not find the source PR for VR {vr}. If this is an import_file_vr, then put that attribute before the 'set_inputs' command.")

        return final_pr
    
    def clean_for_put(self, vr: "Variable" = None, 
                      pr: "source_type" = None, 
                      lookup_vr: "Variable" = None, 
                      lookup_pr: "source_type" = None, 
                      value: Any = None, 
                      show: bool = None,
                      action: Action = None) -> None:
        """Clean the input for the Put object."""
        from ResearchOS.DataObjects.data_object import DataObject                       

        # DataObject attributes
        value = None
        dynamic_vrs = []
        lookup_vrs = []
        all_vrs = []
        if isinstance(value, dict):
            key = [key for key in self.value.keys()][0]
            if key in DataObject.__subclasses__():
                value = {key.prefix: self.value[key]}
            value = json.dumps(value)
        elif vr is not None and vr.hard_coded_value is not None:
            value = json.dumps(vr.hard_coded_value) # Hard-coded value specified in the VR.
        elif value is not None:
            value = json.dumps(value) # Hard-coded value.
        elif vr is not None:
            if pr is not None and not isinstance(pr, list):
                pr = [pr]            
            if lookup_pr is not None and not isinstance(lookup_pr, list):
                lookup_pr = [lookup_pr]
            dynamic_vrs = [Dynamic(vr=vr, pr=pr, action=action, order_num=idx) for idx, pr in enumerate(pr)] if pr is not None else []
            lookup_vrs = [Dynamic(vr=lookup_vr, pr=lookup_pr, action=action, is_lookup = True, order_num=idx) for idx, lookup_pr in enumerate(lookup_pr)] if lookup_vr is not None else []
            all_vrs = dynamic_vrs + lookup_vrs
        self.dynamic_vr_id = [_.id for _ in all_vrs]
        self.is_input = [True for _ in all_vrs]
        self.order_num = [vr.order_num for vr in all_vrs]
        self.is_lookup = [vr.is_lookup for vr in all_vrs]
        self.value = value

        self.dynamic_vrs = dynamic_vrs
        self.lookup_vrs = lookup_vrs