from typing import Union, TYPE_CHECKING, Any
import weakref

if TYPE_CHECKING:
    pass

from ResearchOS.variable import Variable
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action
from ResearchOS.idcreator import IDCreator

from dataclasses import dataclass

class Dynamic():

    instances = weakref.WeakValueDictionary()

    def __new__(cls, *args, **kwargs):
        id = None
        if "id" in kwargs.keys():
            id = kwargs["id"]                    
        if id in Dynamic.instances.keys():
            return Dynamic.instances[id]
        instance = super().__new__(cls)
        if id is not None:
            Dynamic.instances[id] = instance
        return instance
    
    def __init__(self, 
                 id: str = None,
                 vr: "Variable" = None, 
                 pr: Union["Process", "Logsheet"] = None,
                 is_input: bool = True,
                 action: Action = None):
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet              
        if hasattr(self, "id"):
            return # Loaded already
        self.vr = vr
        self.pr = pr
        self.is_input = is_input

        if not vr and not pr and not id:
            self.id = None
            return # Empty Dynamic object
        
        return_conn = False
        if action is None:
            action = Action(name = "Dynamic")
            return_conn = True

        if (vr is None or pr is None) and id is None:
            raise ValueError("Dynamic VR must have specified a Variable and a Process or Logsheet source.")
        
        self.id = id
        if id is None:
            sqlquery_raw = "SELECT dynamic_vr_id FROM dynamic_vrs WHERE vr_id = ? AND pr_id = ? AND is_input = ?"
            params = (vr.id, pr.id, is_input)
            sqlquery = sql_order_result(action, sqlquery_raw, ["id"], single = False, user = True, computer = False)
            result = action.conn.cursor().execute(sqlquery, params).fetchone()
            if result:
                self.id = result[0]
                pr = Process(id=result[1], action=action) if not pr else pr
            else:
                idcreator = IDCreator(action.conn)
                id = idcreator.create_generic_id("dynamic_vrs", "dynamic_vr_id")                                
                if self.pr is not None: # In this case don't add this to the database yet. It will just be an un-identified Dynamic object.
                    self.id = id
                    params = (id, action.id_num, self.vr.id, self.pr.id, self.is_input)
                    action.add_sql_query("None", "dynamic_vrs_insert", params)
        else:
            # Check if the ID previously existed.
            sqlquery = "SELECT dynamic_vr_id, vr_id, pr_id, is_input FROM dynamic_vrs WHERE dynamic_vr_id = ?"
            params = (id,)
            result = action.conn.cursor().execute(sqlquery, params).fetchone()
            if not result and (not vr or not pr):
                raise ValueError(f"Dynamic VR with id {id} not found in database.")
            self.id = id            
            if not self.vr:
                self.vr = Variable(id=result[1], action=action)
            if not self.pr:
                if result[2].startswith("PR"):
                    self.pr = Process(id=result[2], action=action)
                elif result[2].startswith("LG"):
                    self.pr = Logsheet(id=result[2], action=action)
            self.is_input = bool(result[3])
            if not result:
                params = (id, action.id_num, self.vr.id, self.pr.id, self.is_input)
                action.add_sql_query("None", "dynamic_vrs_insert", params)

        if return_conn:
            action.commit = True
            action.execute()
    

@dataclass
class NoneVR():
    vr = None
    pr = None
    show = True

    def __post_init__(self):
        self.sqlquery_raw_select = "SELECT id FROM inputs_outputs WHERE value IS NULL"
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
        self.sqlquery_raw_select = "SELECT id FROM inputs_outputs WHERE value = ?"
        self.params = (self.value,)

# @dataclass
# class HardCodedVR():
#     vr: "Variable" = None
#     value: Any = None

#     def __post_init__(self):
#         self.sqlquery_raw_select = "SELECT id FROM inputs_outputs WHERE vr_id = ? AND value = ?"
#         self.params = (self.vr.id, self.value)

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