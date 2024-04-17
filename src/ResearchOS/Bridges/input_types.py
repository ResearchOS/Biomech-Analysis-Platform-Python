from typing import Union, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ResearchOS.variable import Variable
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet

from dataclasses import dataclass

@dataclass
class NoneVR():
    vr = None
    pr = None

    def __post_init__(self):
        self.sqlquery_raw_select = "SELECT id FROM inputs_outputs WHERE vr_id IS NULL AND pr_id IS NULL AND value IS NULL"
        self.params = ()

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
        pr_id = None
        lookup_pr_id = None
        if self.main_vr.pr:
            pr_id = self.main_vr.pr.id
        if self.lookup_vr and self.lookup_vr.pr:
            lookup_pr_id = self.lookup_vr.pr.id

        if lookup_pr_id:
            sqlquery_raw_select = f"SELECT id FROM inputs_outputs WHERE vr_id = ? AND pr_id = ? AND lookup_vr_id = ? AND lookup_pr_id = ?"
            params = (self.main_vr.vr.id, pr_id, self.lookup_vr.vr.id, lookup_pr_id)
        else:
            sqlquery_raw_select = f"SELECT id FROM inputs_outputs WHERE vr_id = ? AND pr_id = ?"               
            params = (self.main_vr.vr.id, pr_id)
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