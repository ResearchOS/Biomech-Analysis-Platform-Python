from typing import TYPE_CHECKING, Union
import json
import weakref
import logging

if TYPE_CHECKING:
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    from ResearchOS.variable import Variable
    source_type = Union[Process, Logsheet]

from ResearchOS.idcreator import IDCreator
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action

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
                    
        
    def __init__(self, id: int = None,
                 vr: "Variable" = None,
                 pr: "source_type" = None,
                 show: bool = False,
                 action: Action = None):
        if hasattr(self, "id"):
            logger.info(f"Already initialized Port with id {self.id}")
            return
        self.vr = vr
        self.pr = pr
        self.show = show
        self.let = None
        self.action = action
        self.id = None
        self.create_input_or_output()    

    @staticmethod
    def load(id: int, action: Action = None) -> "Port":
        """Load a Port from the database."""
        from ResearchOS.Bridges.input import Input
        from ResearchOS.Bridges.output import Output
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
        lookup_vr = Variable(id=lookup_vr_id, action=action) if lookup_vr_id is not None else None
        lookup_pr = Process(id=lookup_pr_id, action=action) if lookup_pr_id is not None else None
        value = json.loads(value)
        is_input = bool(is_input)
        show = bool(show)
        port = Input(id=id, vr=vr, pr=pr, lookup_vr=lookup_vr, lookup_pr=lookup_pr, value=value, show=show, action=action) if is_input else Output(id=id, vr=vr, pr=pr, show=show, action=action)

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
        try:
            value = json.dumps(value)
        except:
            key = [key for key in value.keys()][0]
            value = json.dumps({key.prefix: value[key]})        
        sqlquery_raw = f"SELECT id FROM inputs_outputs WHERE is_input = ? AND vr_id = ? AND pr_id = ? AND lookup_vr_id = ? AND lookup_pr_id = ? AND value = ?"               
        params = (self.is_input, vr_id, pr_id, lookup_vr_id, lookup_pr_id, value)
        unique_list = ["is_input", "vr_id", "pr_id", "lookup_vr_id", "lookup_pr_id", "value"]
        sqlquery = sql_order_result(action, sqlquery_raw, unique_list, single=True, user = True, computer = False) 

        result = action.conn.execute(sqlquery, params).fetchall()
        if result:
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
        