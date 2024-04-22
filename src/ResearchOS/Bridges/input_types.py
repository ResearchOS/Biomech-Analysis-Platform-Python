from typing import Union, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet   

from ResearchOS.variable import Variable
from ResearchOS.action import Action 

from ResearchOS.Bridges.pipeline_parts import PipelineParts

class Dynamic(PipelineParts):

    cls_name = "Dynamic"
    table_name = "dynamic_vrs"
    id_col = "dynamic_vr_id"
    col_names = ["vr_id", "pr_id"]
    insert_query_name = "dynamic_vrs_insert"
    init_attr_names = ["vr", "pr"]
    
    def __init__(self, 
                 id: str = None,
                 vr_id: "Variable" = None, 
                 pr_id: Union["Process", "Logsheet"] = None,
                 order_num: int = 0,
                 is_lookup: bool = False,
                 vr: Variable = None,
                 pr: Union["Process", "Logsheet"] = None,
                 is_input: bool = True,
                 action: Action = None):
        """Initializes the Dynamic object."""
        super().__init__(id = id, action = action)

        if id:
            self.load_from_db2(id, action)            
            # Make sure that the objects are created.
            # self.vr = Variable(id = self.vr_id, action = action)
            # self.pr = Process(id = self.pr_id, action = action) if self.pr_id.startswith("PR") else Logsheet(id = self.pr_id, action = action)
            return
        
        attrs = {}
        vr_id = vr.id if vr else vr_id
        pr_id = pr.id if pr else pr_id
        attrs["vr_id"] = vr_id
        attrs["pr_id"] = pr_id
        self.init_from_attrs(**attrs, action=action)        
        if not self.id:
            self.get_id_if_present(attrs, action)
            self.assign_id(attrs, action)
            self.save(attrs, action)

        # Helpful but not saved
        self.order_num = order_num
        self.is_lookup = is_lookup
        self.is_input = is_input



        # if vr:
        #     vr_id = vr.id
        # if pr:
        #     pr_id = pr.id
        # if vr_id and not vr:
        #     vr = Variable(id = vr_id, action = action)
        # if pr_id and not pr:
        #     pr = Process(id = pr_id, action = action) if pr_id.startswith("PR") else Logsheet(id = pr_id, action = action)
        # self.vr_id = vr_id
        # self.pr_id = pr_id
        # # Not to be saved in the database with this object.
        # self.order_num = order_num
        # self.is_lookup = is_lookup
        # self.is_input = is_input
        # super().__init__(id = id, action = action)

    def init_from_attrs(self, vr_id: str, pr_id: str, action: Action = None):
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet 
        self.vr = Variable(id = vr_id, action=action) if vr_id is not None else None
        self.pr = None
        if pr_id is not None:
            self.pr = Process(id = pr_id, action=action) if pr_id.startswith("PR") else Logsheet(id = pr_id, action=action)

    # def load_from_db(self, vr_id: str, pr_id: str, action: Action = None):
    #     from ResearchOS.PipelineObjects.process import Process
    #     from ResearchOS.PipelineObjects.logsheet import Logsheet 
    #     self.vr = Variable(id = vr_id, action=action) if vr_id is not None else None
    #     self.pr = None
    #     if pr_id is not None:
    #         self.pr = Process(id = pr_id, action=action) if pr_id.startswith("PR") else Logsheet(id = pr_id, action=action)