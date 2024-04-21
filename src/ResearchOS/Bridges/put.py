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
from ResearchOS.Bridges.pipeline_parts import PipelineParts

logger = logging.getLogger("ResearchOS")

class Put(PipelineParts):
    """Base class for the various Put mechanisms to connect between DiGraphs."""

    cls_name = "Put"
    table_name = "inputs_outputs"
    id_col = "put_id"
    col_names = ["dynamic_vr_id", "is_input", "order_num", "is_lookup", "value"]
    insert_query_name = "pipelineobjects_graph_insert"

    def __init__(self, id: int = None,
                 dynamic_vr_id: list = [],
                 is_input: list = [],
                 order_num: list = [],
                 is_lookup: list = [],
                 value: list = [],
                 action: Action = None):
        self.dynamic_vr_id = dynamic_vr_id
        self.is_input = is_input
        self.order_num = order_num
        self.is_lookup = is_lookup
        self.value = value
        where_str = ""
        params = []
        for idx in enumerate(dynamic_vr_id):
            curr_str = "(dynamic_vr_id = ? AND is_input = ? AND order_num = ? AND is_lookup = ?)"
            if idx == 0:
                where_str += curr_str
            else:
                where_str += " OR " + curr_str
            params += [dynamic_vr_id[idx], int(is_input[idx]), order_num[idx], int(is_lookup[idx])]
        self.params = params
        self.where_str = where_str
        super().__init__(id = id, action = action)

    def load_from_db(self, dynamic_vr_id: list = [],
                     is_input: list = [],
                     order_num: list = [],
                     is_lookup: list = [],
                     value: list = []):
        """Load the dynamic_vr object from the database."""
        if not isinstance(dynamic_vr_id, list):
            dynamic_vr_id = [dynamic_vr_id]
        if not isinstance(is_input, list):
            is_input = [is_input]
        if not isinstance(order_num, list):
            order_num = [order_num]
        if not isinstance(is_lookup, list):
            is_lookup = [is_lookup]
        if not isinstance(value, list):
            value = [value]

        dynamic_vrs = [Dynamic(id = dynamic_vr_id[idx], action = self.action) for idx in range(len(dynamic_vr_id))]
        self.dynamic_vrs = dynamic_vrs
        self.is_input = is_input
        self.order_num = order_num
        self.is_lookup = is_lookup
        self.value = value

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
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    def __init__(self):
        if hasattr(self, "id") and self.id is not None:
            logger.info(f"Already initialized Put with id {self.id}")
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
            sqlquery_raw = f"SELECT io_id FROM lets_puts WHERE dynamic_vr_id IN ({','.join(['?']*len(params))})"
            sqlquery = sql_order_result(action, sqlquery_raw, ["io_id"], single=True, user = True, computer = False)            
        else: # Hard-coded value.
            sqlquery_raw = f"SELECT id FROM inlets_outlets WHERE value = ? AND vr_name_in_code = ? AND ro_id = ? AND is_input = 1" # Specifying is_input=1 in case the "value" is NULL.
            sqlquery = sql_order_result(action, sqlquery_raw, ["value", "vr_name_in_code", "ro_id"], single=True, user = True, computer = False)
            params = (value, self.vr_name_in_code, self.parent_ro.id)
            
        result = action.conn.execute(sqlquery, params).fetchall()
      
        if result:            
            self.id = result[0][0]        
        if self.id is None:
            idcreator = IDCreator(action.conn)
            self.id = idcreator.create_generic_id("inlets_outlets", "id")
            params = (self.id, self.is_input, action.id_num, value, int(self.show), self.parent_ro.id, self.vr_name_in_code)                
            action.add_sql_query(None, "inlets_outlets_insert", params)
        if dynamic_id is not None:
            # Associate the dynamic VR with the input_output.
            for idx, dynamic_vr in enumerate(self.put_value.main_vr):
                is_lookup = self.put_value.lookup_vr is not None
                params = (action.id_num, self.id, dynamic_vr.id, idx, int(is_lookup))
                action.add_sql_query(None, "lets_puts_insert", params)

        Put.instances[self.id] = self

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
            return # Already initialized, loaded from Put.instances

        if id is not None:
            # Run the SQL query to load the values.
            sqlquery_raw = "SELECT id, is_input, value, show, ro_id, vr_name_in_code FROM inlets_outlets WHERE id = ?"
            sqlquery = sql_order_result(action, sqlquery_raw, ["id"], single=True, user = True, computer = False)
            params = (id,)
            result = action.conn.execute(sqlquery, params).fetchall()
            if not result:
                raise ValueError(f"Put with id {id} not found in database.")
            id, is_input, value, show, ro_id, vr_name_in_code = result[0]
            value = json.loads(value)
            sqlquery_raw = "SELECT dynamic_vr_id FROM lets_puts WHERE io_id = ?"
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
        