from typing import TYPE_CHECKING
import weakref

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action

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
            return # Already initialized.
        self.parent_ro = parent_ro
        self.vr_name_in_code = vr_name_in_code
        self.id = None
        self.action = action
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