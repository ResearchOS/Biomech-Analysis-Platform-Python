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
        
        # if isinstance(self.put_value, DynamicMain) and (not self.pr or (self.lookup_vr is not None and self.lookup_pr is None)):
        #     raise ValueError("When creating an Input() object, you must specify a source process or logsheet for the variable.")

        lookup_vr_id = None
        lookup_pr_id = None
        value = None
        if self.pr is None:
            pr_id = None
        else:
            pr = self.pr
            if not isinstance(pr, list):
                pr = [pr]
            pr_id = json.dumps([p.id for p in pr])
        main_dynamic_id = self.put_value.main_vr.id if (hasattr(self.put_value, "main_vr") and self.put_value.main_vr is not None) else None
        lookup_dynamic_id = self.put_value.lookup_vr.id if (hasattr(self.put_value, "lookup_vr") and self.put_value.lookup_vr is not None) else None
        value = self.value      
            
        # In the future this try-except should be more of a class or function. 
        # Right now it just handles the DataObject attribute case because that's the only non-JSON serializable hard-coded value.
        if isinstance(self.put_value, it.DataObjAttr):
            key = [key for key in self.value.keys()][0]
            value = json.dumps({key.prefix: self.value[key]})
        else:
            value = json.dumps(self.value)      

        template_notnull = "{} = ?"
        main_dynamic_id_str = "main_dynamic_vr_id IS NULL"
        lookup_dynamic_id_str = "lookup_dynamic_vr_id IS NULL"
        params = [int(self.is_input)]
        unique_list = ["is_input"]
        if main_dynamic_id is not None:
            main_dynamic_id_str = template_notnull.format("main_dynamic_vr_id")
            params.append(main_dynamic_id)
            unique_list.append("main_dynamic_vr_id")
        if lookup_dynamic_id is not None:
            lookup_dynamic_id_str = template_notnull.format("lookup_dynamic_vr_id")
            params.append(lookup_dynamic_id)
            unique_list.append("lookup_dynamic_vr_id")
        params.append(value)
        params.append(self.vr_name_in_code)
        params.append(self.parent_ro.id)
        params = tuple(params)
        unique_list.append("value")
        unique_list.append("vr_name_in_code")
        unique_list.append("ro_id")

        sqlquery_raw = f"SELECT id FROM inputs_outputs WHERE is_input = ? AND {main_dynamic_id_str} AND {lookup_dynamic_id_str} AND value = ? AND vr_name_in_code = ? AND ro_id = ?"                               
        sqlquery = sql_order_result(action, sqlquery_raw, unique_list, single=True, user = True, computer = False) 

        result = action.conn.execute(sqlquery, params).fetchall()        
        if result:            
            self.id = result[0][0]
        if self.id is None:
            idcreator = IDCreator(action.conn)
            self.id = idcreator.create_generic_id("inputs_outputs", "id")
            params = (self.id, self.is_input, action.id_num, main_dynamic_id, lookup_dynamic_id, value, int(self.show), self.parent_ro.id, self.vr_name_in_code)                
            action.add_sql_query(None, "inputs_outputs_insert", params)

        Port.instances[self.id] = self

        if return_conn:
            action.commit = True
            action.execute()
        