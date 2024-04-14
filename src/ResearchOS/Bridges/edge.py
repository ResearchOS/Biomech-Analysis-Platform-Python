from ResearchOS.Bridges.inlet import Inlet
from ResearchOS.Bridges.outlet import Outlet
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.action import Action

class Edge():

    def __str__(self):
        return f"{self.src.id} -> {self.dest.id}"
    
    def __init__(self, inlet: Inlet, outlet: Outlet, action: Action = None):
        self.src = outlet
        self.dest = inlet
        self.id = None
        return_conn = False
        if action is None:
            pool = SQLiteConnectionPool()
            return_conn = True
            action = Action(name = "create_edge")

        sqlquery_raw = "SELECT connection_id FROM connections WHERE outlet_id = ? AND inlet_id = ? AND is_active = 1"
        sqlquery = sql_order_result(inlet.action, sqlquery_raw, [outlet.id, inlet.id], single=True, user = True, computer = False)
        params = (outlet.id, inlet.id)
        result = action.conn.execute(sqlquery, params).fetchall()
        if result:
            self.id = result[0][0]
        else:
            sqlquery = "INSERT INTO connections (outlet_id, inlet_id, action_id_num) VALUES (?, ?, ?)"
            params = (outlet.id, inlet.id, action.id)
            action.conn.execute(sqlquery, params)
            self.id = action.conn.lastrowid

        if return_conn:
            action.commit = True
            action.execute()            

    