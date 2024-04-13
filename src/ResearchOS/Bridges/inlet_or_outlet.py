from typing import TYPE_CHECKING
import weakref
import logging

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action
from ResearchOS.Bridges.port import Port

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
            return # Already initialized.
        self.parent_ro = parent_ro
        self.vr_name_in_code = vr_name_in_code
        self.id = None
        self.action = action
        self.puts = []
        self.create_inlet_or_outlet()

    def create_inlet_or_outlet(self):
        """Creates the inlet or outlet in the database, and stores the reference to the instance."""
        return_conn = False
        action = self.action
        if action is None:            
            return_conn = True
            action = Action(name = f"create_inlet_or_outlet")

        sqlquery_raw = f"SELECT id FROM inlets_outlets WHERE is_inlet = ? AND pl_object_id = ? AND vr_name_in_code = ? AND is_active = 1"
        sqlquery = sql_order_result(action, sqlquery_raw, ["is_inlet", "pl_object_id", "vr_name_in_code"], single=True, user = True, computer = False)
        params = (self.is_inlet, self.parent_ro.id, self.vr_name_in_code)
        result = action.conn.execute(sqlquery, params).fetchall()
        if result:
            self.id = result[0][0]
        else:
            sqlquery = f"INSERT INTO inlets_outlets (is_inlet, pl_object_id, vr_name_in_code, action_id_num) VALUES (?, ?, ?, ?)"
            params = (self.is_inlet, self.parent_ro.id, self.vr_name_in_code, action.id)
            cursor = action.conn.cursor()
            cursor.execute(sqlquery, params)
            self.id = cursor.lastrowid

        InletOrOutlet.instances[self.id] = self

        if return_conn:
            pool = SQLiteConnectionPool()
            pool.return_connection(action.conn)

    def add_put(self, put: Port, action: Action = None):
        """Adds an In/Output to the In/Outlet."""
        self.puts.append(put)
        put.let = self
        # Make the change in the database
        sqlquery = "INSERT INTO lets_puts (let_id, put_id) VALUES (?, ?)"

        return_conn = False
        if action is None:
            action = Action(name="add_put to let")
            return_conn = True
            action.conn.execute(sqlquery, (self.id, put.id))
        else:
            action.add_sqlquery(sqlquery, (self.id, put.id))
        
        if return_conn:
            pool = SQLiteConnectionPool()            
            pool.return_connection(action.conn)

    def remove_put(self, put: Port, action: Action = None):
        """Removes an In/Output from the In/Outlet."""
        if put not in self.puts:
            logger.info(f"{put.__class__.__name__} {put.id} is not in the list of puts of {self.__class__.__name__} {self.id}")
            return
        self.puts.remove(put)
        put.let = None
        # Make the change in the database
        sqlquery = "INSERT INTO lets_puts (let_id, put_id, is_active) VALUES (?, ?, ?)"

        return_conn = False
        if action is None:
            action = Action(name="remove_put from let")
            return_conn = True
            action.conn.execute(sqlquery, (self.id, put.id, 0))
        else:
            action.add_sqlquery(sqlquery, (self.id, put.id, 0))
        
        if return_conn:
            pool = SQLiteConnectionPool()            
            pool.return_connection(action.conn)