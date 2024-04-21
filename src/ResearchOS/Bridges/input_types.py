from typing import Union, TYPE_CHECKING, Any
# import weakref

if TYPE_CHECKING:
    pass

from ResearchOS.variable import Variable
# from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action
# from ResearchOS.idcreator import IDCreator

from dataclasses import dataclass
from ResearchOS.Bridges.pipeline_parts import PipelineParts

class Dynamic(PipelineParts):

    cls_name = "Dynamic"
    table_name = "dynamic_vrs"
    id_col = "dynamic_vr_id"
    col_names = ["vr_id", "pr_id"]
    insert_query_name = "dynamic_vrs_insert"

    
    def __init__(self, 
                 id: str = None,
                 vr_id: "Variable" = None, 
                 pr_id: Union["Process", "Logsheet"] = None,
                 action: Action = None):
        # from ResearchOS.PipelineObjects.process import Process
        # from ResearchOS.PipelineObjects.logsheet import Logsheet      

        self.vr_id = vr_id
        self.pr_id = pr_id
        super().__init__(id = id, action = action)
    

@dataclass
class NoneVR():
    vr = None
    pr = None
    show = True

    def __post_init__(self):
        self.sqlquery_raw_select = "SELECT id FROM inlets_outlets WHERE value IS NULL"
        self.params = ()

class DynamicMain():    

    def __init__(self, main_vr: list, lookup_vr: list = None, show: bool = False):
        if not isinstance(main_vr, list):
            main_vr = [main_vr]
        self.main_vr = main_vr
        self.lookup_vr = lookup_vr
        self.show = show        
    
    def __post_init__(self):
        lookup_pr_ids = tuple([_.id for _ in self.lookup_vr]) if self.lookup_vr else tuple()
        params = tuple([_.id for _ in self.main_vr]) + lookup_pr_ids
        sqlquery_raw_select = f"SELECT io_id FROM inputs_outputs_dynamics WHERE dynamic_vr_id IN ({','.join(['?']*len(params))})"                     
        self.params = params
        self.sqlquery_raw_select = sqlquery_raw_select

@dataclass
class HardCoded():
    value: Any = None
    show: bool = True

    def __post_init__(self):
        self.sqlquery_raw_select = "SELECT id FROM inlets_outlets WHERE value = ?"
        self.params = (self.value,)

@dataclass
class ImportFile():
    value: Any = None

    def __post_init__(self):
        self.sqlquery_raw_select = "SELECT id FROM inlets_outlets WHERE value = ?"
        self.params = (self.value,)

@dataclass
class DataObjAttr():
    cls: type = None
    attr: str = None

    def __post_init__(self):
        self.sqlquery_raw_select = "SELECT id FROM inlets_outlets WHERE value = ?"
        self.params = ({self.cls.prefix: self.attr},)