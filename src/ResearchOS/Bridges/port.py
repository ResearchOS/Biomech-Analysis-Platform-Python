from typing import TYPE_CHECKING, Union, Any
import json
import weakref
import logging

if TYPE_CHECKING:
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    from ResearchOS.variable import Variable
    from ResearchOS.research_object import ResearchObject
    source_type = Union[Process, Logsheet]

from ResearchOS.idcreator import IDCreator
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action
import ResearchOS.Bridges.input_types as it
from ResearchOS.Bridges.input_types import Dynamic

logger = logging.getLogger("ResearchOS")

class Port():
    """Base class for the various port mechanisms to connect between DiGraphs."""

    instances = weakref.WeakValueDictionary()

    def __new__(cls, *args, **kwargs):
        id = None
        if "id" in kwargs.keys():
            id = kwargs["id"]                    
        if id in Port.instances.keys():
            return Port.instances[id]
        instance = super().__new__(cls)
        if id is not None:
            Port.instances[id] = instance
        return instance
                    
        
    def __init__(self):
        if hasattr(self, "id") and self.id is not None:
            logger.info(f"Already initialized Port with id {self.id}")
            return
        self.vr = None
        self.pr = None
        self.id = self._id
        del self._id
        self.value = None
        self.lookup_pr = None
        self.lookup_vr = None
        self.show = False
        put = self.put_value
        if put.__class__ == it.HardCoded:
            self.value = put.value
            self.show = True
        elif put.__class__ == it.ImportFile:
            self.value = put.value
            self.show = True
        elif put.__class__ == it.DataObjAttr:
            self.value = {put.cls: put.attr}
            self.show = True
        elif put.__class__ == it.DynamicMain:
            self.vr = [vr.vr for vr in put.main_vr]
            if len(self.vr) == 1:
                self.vr = self.vr[0]
            self.pr = [pr.pr for pr in put.main_vr]
            if put.lookup_vr and put.lookup_vr.vr is not None:
                self.lookup_vr = [vr.vr for vr in put.lookup_vr]
                if len(self.lookup_vr) == 1:
                    self.lookup_vr = self.lookup_vr[0]
                self.lookup_pr = [pr.pr for pr in put.lookup_vr]
            self.show = put.show            
        elif put.__class__ == it.NoneVR:
            self.show = True
        if self.id is not None:
            return
        self.create_input_or_output() 
        if self.return_conn:
            self.action.commit = True
            self.action.execute()  


    def create_input_or_output(self) -> None:
        """Creates the input or output in the database, and stores the reference to the instance."""
        # This happens when the Input is called directly by the user. Not None when called by internal functions.
        if self.parent_ro is None or self.vr_name_in_code is None:
            return
        
        return_conn = False
        action = self.action
        if action is None:
            return_conn = True
            action = Action(name = f"create_input_or_output")        

        dynamic_id = [_.id for _ in self.put_value.main_vr] if (hasattr(self.put_value, "main_vr") and self.put_value.main_vr is not None) else None
        value = self.value      
            
        # In the future this try-except should be more of a class or function. 
        # Right now it just handles the DataObject attribute case because that's the only non-JSON serializable hard-coded value.
        if isinstance(self.put_value, it.DataObjAttr):
            key = [key for key in self.value.keys()][0]
            value = json.dumps({key.prefix: self.value[key]})
        else:
            value = json.dumps(self.value)      
 
        # Dynamic VR.
        if dynamic_id is not None:
            params = tuple([_.id for _ in self.put_value.main_vr])
            sqlquery_raw = f"SELECT io_id FROM inputs_outputs_to_dynamic_vrs WHERE dynamic_vr_id IN ({','.join(['?']*len(params))})"
            sqlquery = sql_order_result(action, sqlquery_raw, ["io_id"], single=True, user = True, computer = False)            
        else: # Hard-coded value.
            sqlquery_raw = f"SELECT id FROM inputs_outputs WHERE value = ? AND vr_name_in_code = ? AND ro_id = ? AND is_input = 1" # Specifying is_input=1 in case the "value" is NULL.
            sqlquery = sql_order_result(action, sqlquery_raw, ["value", "vr_name_in_code", "ro_id"], single=True, user = True, computer = False)
            params = (value, self.vr_name_in_code, self.parent_ro.id)
            
        result = action.conn.execute(sqlquery, params).fetchall()
      
        if result:            
            self.id = result[0][0]        
        if self.id is None:
            idcreator = IDCreator(action.conn)
            self.id = idcreator.create_generic_id("inputs_outputs", "id")
            params = (self.id, self.is_input, action.id_num, value, int(self.show), self.parent_ro.id, self.vr_name_in_code)                
            action.add_sql_query(None, "inputs_outputs_insert", params)
        if dynamic_id is not None:
            # Associate the dynamic VR with the input_output.
            for idx, dynamic_vr in enumerate(self.put_value.main_vr):
                is_lookup = self.put_value.lookup_vr is not None
                params = (action.id_num, self.id, dynamic_vr.id, idx, int(is_lookup))
                action.add_sql_query(None, "inputs_outputs_to_dynamic_vrs_insert", params)

        Port.instances[self.id] = self

        if return_conn:
            action.commit = True
            action.execute()

    def init_helper(self, id: int = None,
                 vr: "Variable" = None, 
                 pr: "source_type" = None,
                 lookup_vr: "Variable" = None,
                 lookup_pr: "source_type" = None,
                 value: Any = None,
                 show: bool = False,
                 action: "Action" = None,
                 parent_ro: "ResearchObject" = None,
                 vr_name_in_code: str = None,
                 **kwargs):
        
        return_conn = False
        if action is None:
            action = Action(name = "Set_Inputs")
            return_conn = True

        if hasattr(self, "id"):
            return # Already initialized, loaded from Port.instances

        if id is not None:
            # Run the SQL query to load the values.
            sqlquery_raw = "SELECT id, is_input, value, show, ro_id, vr_name_in_code FROM inputs_outputs WHERE id = ?"
            sqlquery = sql_order_result(action, sqlquery_raw, ["id"], single=True, user = True, computer = False)
            params = (id,)
            result = action.conn.execute(sqlquery, params).fetchall()
            if not result:
                raise ValueError(f"Port with id {id} not found in database.")
            id, is_input, value, show, ro_id, vr_name_in_code = result[0]
            value = json.loads(value)
            sqlquery_raw = "SELECT dynamic_vr_id FROM inputs_outputs_to_dynamic_vrs WHERE io_id = ?"
            sqlquery = sql_order_result(action, sqlquery_raw, ["id"], single=False, user = True, computer = False)
            params = (id,)
            result = action.conn.execute(sqlquery, params).fetchall()
            if result:
                dynamic_vr_ids = [row[0] for row in result]
                dynamics = [Dynamic(id=dynamic_vr_id, action=action) for dynamic_vr_id in dynamic_vr_ids]
            vr = [d.vr for d in dynamics if not bool(d.is_lookup)] if result else None
            if vr is not None and len(vr)==1:
                vr = vr[0]
            pr = [d.pr for d in dynamics if not bool(d.is_lookup)] if result else None
            if pr is not None and len(pr)==1:
                pr = pr[0]
            lookup_vr = [d.vr for d in dynamics if bool(d.is_lookup)] if result else None
            if lookup_vr is not None and len(lookup_vr)==1:
                lookup_vr = lookup_vr[0]
            lookup_pr = [d.pr for d in dynamics if not bool(d.is_lookup)] if result else None
            is_input = bool(is_input)
            show = bool(show)
            parent_ro = Process(id = ro_id, action = action) if ro_id.startswith("PR") else Logsheet(id = ro_id, action = action)
        
        # Now whether loading or saving, all inputs are properly shaped.
        if isinstance(value, dict) and (vr is None or vr.hard_coded_value is None):
            key = list(value.keys())[0]
            if key.__class__ == type:
                input = it.DataObjAttr(key, value[key])
            else:
                input = it.HardCoded(value)
        elif hasattr(parent_ro, "import_file_vr_name") and vr_name_in_code==parent_ro.import_file_vr_name:
            import_value_default = "import_file_vr_name"   
            input = it.ImportFile(import_value_default)
        elif vr is not None and vr.hard_coded_value is not None:
            input = it.HardCoded(value=vr.hard_coded_value)
        elif value is not None:
            # 2. hard-coded value.
            input = it.HardCoded(value)        
        elif vr is not None:
            # 3. dynamic value. Also the import_file_vr_name would fit here because it's a VR, but that gets overwritten in the VRHandler.
            if not isinstance(pr, list):
                pr = [pr]
            if lookup_pr is not None and not isinstance(lookup_pr, list):
                lookup_pr = [lookup_pr]
            dynamics = [it.Dynamic(vr=vr, pr=pr, action=action) for pr in pr] if pr else None
            lookups = [it.Dynamic(vr=lookup_vr, pr=lookup_pr, action=action) for lookup_pr in lookup_pr] if lookup_vr is not None else None
            input = it.DynamicMain(dynamics, lookups, show=show)
        else:
            input = it.NoneVR()

        self.vr_name_in_code = vr_name_in_code
        self.put_value = input
        self.action = action
        self.parent_ro = parent_ro
        self._id = id
        self.return_conn = return_conn
        