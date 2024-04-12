from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject  
    from action import Action

from sqlite_pool import SQLiteConnectionPool
from sql.sql_runner import sql_order_result
from action import Action

class InletOrOutlet():

    def __init__(self, parent_ro: "ResearchObject", vr_name_in_code: str, action: "Action" = None):
        self.parent_ro = parent_ro
        self.vr_name_in_code = vr_name_in_code
        self.id = None
        self.action = action
        self.create_inlet_or_outlet()

    def create_inlet_or_outlet(self):
        return_conn = False
        action = self.action
        if action is None:
            pool = SQLiteConnectionPool()
            return_conn = True
            action = Action(name = f"create_{self.table_name}")

        sqlquery_raw = f"SELECT {self.id_col} FROM {self.table_name} WHERE pr_id = ? AND vr_name_in_code = ? AND is_active = 1"
        sqlquery = sql_order_result(action, sqlquery_raw, [self.parent_ro.pr_id, self.vr_name_in_code], single=True, user = True, computer = False)
        result = action.conn.execute(sqlquery).fetchall()
        if result:
            self.id = result[0][0]
        else:
            sqlquery = f"INSERT INTO {self.table_name} (pr_id, vr_name_in_code, action_id_num) VALUES (?, ?, ?)"
            params = (self.parent_ro.id, self.vr_name_in_code, action.id)
            action.conn.execute(sqlquery, params)
            self.id = action.conn.lastrowid

        if return_conn:
            pool.return_connection(action.conn)