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
        if id in cls.instances.keys():
            return cls.instances[id]
        return super().__new__(cls)
                    
        
    def __init__(self):
        if hasattr(self, "id") and self.id is not None:
            logger.info(f"Already initialized Port with id {self.id}")
            return
        self.vr = None
        self.pr = None
        self.id = None
        self.value = None
        self.lookup_pr = None
        self.lookup_vr = None
        self.show = False
        put = self.put_value
        if put.__class__ == it.HardCoded:
            self.value = put.value
            self.show = True
        elif put.__class__ == it.HardCodedVR:
            self.vr = put.vr
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
        is_new = self.create_input_or_output()   
        return is_new 

    @staticmethod
    def load(id: int, action: Action = None, let="InletOrOutlet") -> "Port":
        """Load a Port from the database."""
        from ResearchOS.Bridges.input import Input
        from ResearchOS.Bridges.output import Output
        from ResearchOS.variable import Variable
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        if id in Port.instances.keys():
            return Port.instances[id]
        sqlquery_raw = "SELECT id, is_input, vr_id, pr_id, lookup_vr_id, lookup_pr_id, value, show FROM inputs_outputs WHERE id = ?"
        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name = f"load_port")

        sqlquery = sql_order_result(action, sqlquery_raw, ["id"], single=True, user = True, computer = False)
        params = (id,)
        result = action.conn.execute(sqlquery, params).fetchall()
        if not result:
            raise ValueError(f"Port with id {id} not found in database.")
        id, is_input, vr_id, pr_id, lookup_vr_id, lookup_pr_id, value, show = result[0]
        if not pr_id:
            pr_id = json.dumps([])
        value = json.loads(value)
        vr = Variable(id=vr_id, action=action) if vr_id is not None else None
        pr = []
        for p in json.loads(pr_id):
            if p is None:
                pr = None
                break
            if p.startswith("PR"):
                pr.append(Process(id=p, action=action))
            elif p.startswith("LG"):
                pr.append(Logsheet(id=p, action=action))
        if len(pr)==1:
            pr = pr[0]
        elif len(pr)==0:
            pr = None
        lookup_vr = Variable(id=lookup_vr_id, action=action) if lookup_vr_id is not None else None
        lookup_pr = Process(id=lookup_pr_id, action=action) if lookup_pr_id is not None else None
        is_input = bool(is_input)
        show = bool(show)
        port = Input(vr=vr, pr=pr, lookup_vr=lookup_vr, lookup_pr=lookup_pr, value=value, show=show, action=action) if is_input else Output(id=id, vr=vr, pr=pr, show=show, action=action)
        port.id = id

        if return_conn:
            action.execute()

        return port


    def add_attrs(self, parent_ro: "source_type", vr_name_in_code: str) -> None:
        """Add the attributes about the Process/Logsheet parent object to the Port object.
        This has to be done in a separate step because the Input or Output is resolved before the "set_inputs/outputs" helper functions."""
        self.parent_ro = parent_ro
        self.vr_name_in_code = vr_name_in_code


    def create_input_or_output(self) -> None:
        """Creates the input or output in the database, and stores the reference to the instance."""
        return_conn = False
        action = self.action
        if action is None:
            return_conn = True
            action = Action(name = f"create_input_or_output")

        # ID is provided, so load the object.
        if self.id is not None:
            obj = Port.load(self.id, action=action)
            self.__dict__.update(obj.__dict__)
            Port.instances[self.id] = self
            return

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
        vr_id = self.vr.id if self.vr is not None else None
        if self.is_input:            
            lookup_vr_id = self.lookup_vr.id if self.lookup_vr is not None else None
            lookup_pr_id = self.lookup_pr.id if self.lookup_pr is not None else None        
            value = self.value 
        # In the future this try-except should be more of a class or function. 
        # Right now it just handles the DataObject attribute case because that's the only non-JSON serializable hard-coded value.
        if isinstance(self.put_value, it.DataObjAttr):
            key = [key for key in self.value.keys()][0]
            value = json.dumps({key.prefix: self.value[key]})
        else:
            value = json.dumps(self.value)      

        template_notnull = "{} = ?"
        vr_id_str = "vr_id IS NULL"
        pr_id_str = "pr_id IS NULL"
        lookup_vr_id_str = "lookup_vr_id IS NULL"
        lookup_pr_id_str = "lookup_pr_id IS NULL"
        params = [int(self.is_input)]
        unique_list = ["is_input"]
        if vr_id is not None:
            vr_id_str = template_notnull.format("vr_id")
            params.append(vr_id)
            unique_list.append("vr_id")
        if pr_id is not None:
            pr_id_str = template_notnull.format("pr_id")
            params.append(pr_id)
            unique_list.append("pr_id")
        if lookup_vr_id is not None:
            lookup_vr_id_str = template_notnull.format("lookup_vr_id")
            params.append(lookup_vr_id)
            unique_list.append("lookup_vr_id")
        if lookup_pr_id is not None:
            lookup_pr_id_str = template_notnull.format("lookup_pr_id")
            params.append(lookup_pr_id)
            unique_list.append("lookup_pr_id")
        params.append(value)
        params = tuple(params)
        unique_list.append("value")

        sqlquery_raw = f"SELECT id FROM inputs_outputs WHERE is_input = ? AND {vr_id_str} AND {pr_id_str} AND {lookup_vr_id_str} AND {lookup_pr_id_str} AND value = ?"                               
        sqlquery = sql_order_result(action, sqlquery_raw, unique_list, single=True, user = True, computer = False) 

        result = action.conn.execute(sqlquery, params).fetchall()
        is_new = True
        if result:
            is_new = False
            self.id = result[0][0]
        if self.id is None:
            idcreator = IDCreator(action.conn)
            self.id = idcreator.create_generic_id("inputs_outputs", "id")
            params = (self.id, self.is_input, action.id_num, vr_id, pr_id, lookup_vr_id, lookup_pr_id, value, int(self.show))                
            action.add_sql_query(None, "inputs_outputs_insert", params)

        Port.instances[self.id] = self

        if return_conn:
            action.commit = True
            action.execute()

        return is_new
        