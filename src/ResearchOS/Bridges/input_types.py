from typing import Union, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet

from dataclasses import dataclass

@dataclass
class Dynamic():
    vr: "Variable" = None
    pr: Union["Process", "Logsheet"] = None

@dataclass
class DynamicMain():    
    main_vr: Dynamic = None
    lookup_vr: Dynamic = None
    show: bool = False
    
    def __post_init__(self):
        from ResearchOS.Bridges.input import Input
        if self.lookup_vr.vr:
            sqlquery_raw_select = f"SELECT id FROM inputs_outputs WHERE vr_id = ? AND pr_id = ? AND lookup_vr_id = ? AND lookup_pr_id = ?"
            params = (self.main_vr.vr.id, self.main_vr.pr.id, self.lookup_vr.vr.id, self.lookup_vr.pr.id)
        else:
            sqlquery_raw_select = f"SELECT id FROM inputs_outputs WHERE vr_id = ? AND pr_id = ?"
            if not self.main_vr.pr:                                            
                self.main_vr.pr = Input.set_source_pr(self.main_vr.vr)                
            params = (self.main_vr.vr.id, self.main_vr.pr.id)
        self.params = params
        self.sqlquery_raw_select = sqlquery_raw_select

@dataclass
class HardCoded():
    value: Any = None
    show: bool = True

    def __post_init__(self):
        self.sqlquery_raw_select = "SELECT id FROM inputs_outputs WHERE value = ?"
        self.params = (self.value,)

@dataclass
class HardCodedVR():
    vr: "Variable" = None
    value: Any = None

    def __post_init__(self):
        self.sqlquery_raw_select = "SELECT id FROM inputs_outputs WHERE vr_id = ? AND value = ?"
        self.params = (self.vr.id, self.value)

@dataclass
class ImportFile():
    value: Any = None

    def __post_init__(self):
        self.sqlquery_raw_select = "SELECT id FROM inputs_outputs WHERE value = ?"
        self.params = (self.value,)

@dataclass
class DataObjAttr():
    cls: type = None
    attr: str = None

    def __post_init__(self):
        self.sqlquery_raw_select = "SELECT id FROM inputs_outputs WHERE value = ?"
        self.params = ({self.cls.prefix: self.attr},)