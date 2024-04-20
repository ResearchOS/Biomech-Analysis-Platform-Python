from typing import TYPE_CHECKING, Union
import json
import weakref
import logging

if TYPE_CHECKING:
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    source_type = Union[Process, Logsheet]

from ResearchOS.idcreator import IDCreator
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action
import ResearchOS.Bridges.input_types as it

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
            self.vr = put.main_vr.vr
            self.pr = put.main_vr.pr
            if put.lookup_vr and put.lookup_vr.vr is not None:
                self.lookup_vr = put.lookup_vr.vr
                self.lookup_pr = put.lookup_vr.pr
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
        from ResearchOS.Bridges.input_types import DynamicMain
        return_conn = False
        action = self.action
        if action is None:
            return_conn = True
            action = Action(name = f"create_input_or_output")

        # This happens when the Input is called directly by the user. Not None when called by internal functions.
        if self.parent_ro is None or self.vr_name_in_code is None:
            return

        dynamic_ids = self.put_value.main_vr.id if (hasattr(self.put_value, "main_vr") and self.put_value.main_vr is not None) else None
        value = self.value      
            
        # In the future this try-except should be more of a class or function. 
        # Right now it just handles the DataObject attribute case because that's the only non-JSON serializable hard-coded value.
        if isinstance(self.put_value, it.DataObjAttr):
            key = [key for key in self.value.keys()][0]
            value = json.dumps({key.prefix: self.value[key]})
        else:
            value = json.dumps(self.value)      
 
        # Dynamic VR.
        if main_dynamic_id is not None:
            main_dynamic_id_str = f"{main_dynamic_vr_id} = {', '.join(['?' for _ in dynamic_ids])}"
            params.append(main_dynamic_id)
            unique_list.append("main_dynamic_vr_id")
            sqlquery_raw = f"SELECT io_id FROM inputs_outputs_dynamic WHERE is_active = 1 AND dynamic_vr_id IN {main_dynamic_id_str}"
            sqlquery = sql_order_result(action, sqlquery_raw, ["io_id"], single=True, user = True, computer = False)            
        else: # Hard-coded value.
            sqlquery_raw = f"SELECT id FROM inputs_outputs WHERE is_active = 1 AND value = ? AND vr_name_in_code = ? AND ro_id = ?"
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

        Port.instances[self.id] = self

        if return_conn:
            action.commit = True
            action.execute()
        