from typing import TYPE_CHECKING
import weakref
import logging

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action
from ResearchOS.Bridges.port import Port
from ResearchOS.idcreator import IDCreator

logger = logging.getLogger("ResearchOS")

class InletOrOutlet():

    instances = weakref.WeakValueDictionary()

    def __new__(cls, *args, **kwargs):
        if "id" in kwargs.keys():
            id = kwargs["id"]
        else:
            return super().__new__(cls)
        if id in cls.instances.keys():
            return cls.instances[id]

    def __init__(self, parent_ro: "ResearchObject", vr_name_in_code: str, action: "Action" = None):
        """Initializes the Inlet or Outlet object."""
        if hasattr(self, "id"):
            logger.info(f"Already initialized InletOrOutlet with id {self.id}")
            return
        self.parent_ro = parent_ro
        self.vr_name_in_code = vr_name_in_code
        self.action = action
        self.puts = []
        self.id = None
        self.create_inlet_or_outlet()

    @staticmethod
    def load(id: int, action: Action = None) -> "InletOrOutlet":
        """Load an Inlet or Outlet from the database."""
        from ResearchOS.Bridges.inlet import Inlet
        from ResearchOS.Bridges.outlet import Outlet
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        if id in InletOrOutlet.instances.keys():
            return InletOrOutlet.instances[id]
        sqlquery_raw = "SELECT id, is_inlet, pl_object_id, vr_name_in_code FROM inlets_outlets WHERE id = ?"
        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name = f"load_inlet_or_outlet")
        sqlquery = sql_order_result(action, sqlquery_raw, ["id"], single=True, user = True, computer = False)
        params = (id,)
        result = action.conn.execute(sqlquery, params).fetchall()
        if not result:
            raise ValueError(f"InletOrOutlet with id {id} not found in the database")        
        id, is_inlet, pl_object_id, vr_name_in_code = result[0]        
        if pl_object_id is None:
            parent_ro = None
        elif pl_object_id.startswith("PR"):
            parent_ro = Process(id=pl_object_id, action=action)
        elif pl_object_id.startswith("LG"):
            parent_ro = Logsheet(id=pl_object_id, action=action)
        inlet_or_outlet = Inlet(parent_ro, vr_name_in_code, action) if is_inlet else Outlet(parent_ro, vr_name_in_code, action)
        inlet_or_outlet.id = id
        inlet_or_outlet.parent_ro = parent_ro
        inlet_or_outlet.vr_name_in_code = vr_name_in_code
        InletOrOutlet.instances[id] = inlet_or_outlet
        return inlet_or_outlet        


    def create_inlet_or_outlet(self):
        """Creates the inlet or outlet in the database, and stores the reference to the instance."""
        return_conn = False
        action = self.action
        if action is None:            
            return_conn = True
            action = Action(name = f"create_inlet_or_outlet")

        sqlquery_raw = f"SELECT id FROM inlets_outlets WHERE is_inlet = ? AND pl_object_id = ? AND vr_name_in_code = ?"
        sqlquery = sql_order_result(action, sqlquery_raw, ["is_inlet", "pl_object_id", "vr_name_in_code"], single=True, user = True, computer = False)
        params = (self.is_inlet, self.parent_ro.id, self.vr_name_in_code)
        result = action.conn.execute(sqlquery, params).fetchall()
        if result:
            self.id = result[0][0]
        if self.id is None:
            idcreator = IDCreator(action.conn)
            self.id = idcreator.create_generic_id("inlets_outlets", "id")

            params = (self.id, self.is_inlet, self.parent_ro.id, self.vr_name_in_code, action.id_num)
            action.add_sql_query(None, "inlets_outlets_insert", params)            

        InletOrOutlet.instances[self.id] = self

        if return_conn:
            action.commit = True
            action.execute()


    def add_put(self, put: Port, action: Action = None, do_insert: bool = True):
        """Adds an In/Output to the In/Outlet."""
        self.puts.append(put)
        put.let = self
        if not do_insert:
            return
        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name="add_put to let")
        sqlquery = "SELECT id FROM lets_puts WHERE let_id = ? AND put_id = ?"
        params = (self.id, put.id)
        cursor = action.conn.cursor()
        result = cursor.execute(sqlquery, params).fetchall()
        if result:
            put_let_id = result[0][0]
        else:
            put_let_id = IDCreator(action.conn).create_generic_id("lets_puts", "id")          

        params = (put_let_id, action.id_num, self.id, put.id, 1)
        action.add_sql_query(None, "let_put_insert", params)
        if return_conn:
            action.commit = True
            action.execute()


    def remove_put(self, put: Port, action: Action = None, do_insert: bool = True):
        """Removes an In/Output from the In/Outlet."""
        if put not in self.puts:
            logger.info(f"{put.__class__.__name__} {put.id} is not in the list of puts of {self.__class__.__name__} {self.id}")
            return
        self.puts.remove(put)
        put.let = None
        if not do_insert:
            return
        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name="remove_put from let")
        # Get put_let_id
        sqlquery = action.queries["let_put_select"]
        params = (self.id, put.id)
        cursor = action.conn.cursor()
        result = cursor.execute(sqlquery, params).fetchall()
        if result:
            put_let_id = result[0][0]
        else:
            logger.info(f"Put {put.id} is not in the list of puts of {self.__class__.__name__}")
            return
        
        # Make the change in the database        
        params = (put_let_id, action.id_num, self.id, put.id, 0)
        action.add_sql_query(None, "let_put_insert", params)        
        action.execute(return_conn)
            