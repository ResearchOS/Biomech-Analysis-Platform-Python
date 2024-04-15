import weakref

from ResearchOS.Bridges.inlet import Inlet
from ResearchOS.Bridges.outlet import Outlet
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action

class Edge():

    instances = weakref.WeakValueDictionary()

    def __new__(cls, *args, **kwargs):
        id = None
        if "id" in kwargs.keys():
            id = kwargs["id"]                    
        if id in cls.instances.keys():
            return cls.instances[id]
        return super().__new__(cls)

    def __str__(self):
        return f"""{self.src.parent_ro.id} "{self.src.vr_name_in_code}" -> {self.dest.parent_ro.id} "{self.dest.vr_name_in_code}"."""
    
    @staticmethod
    def load(id: int, action: Action = None) -> "Edge":
        if id in Edge.instances.keys():
            return Edge.instances[id]
        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name = f"load_edge")
        sqlquery_raw = "SELECT id, outlet_id, inlet_id FROM connections WHERE id = ?"
        sqlquery = sql_order_result(action, sqlquery_raw, ["id"], single=True, user = True, computer = False)
        params = (id,)
        result = action.conn.execute(sqlquery, params).fetchall()
        if not result:
            raise ValueError(f"Edge with id {id} not found in database.")
        id, outlet_id, inlet_id = result[0]
        outlet = Outlet.load(outlet_id)
        inlet = Inlet.load(inlet_id)
        return Edge(id=id, outlet=outlet, inlet=inlet)
    
    def __init__(self, inlet: Inlet, outlet: Outlet, action: Action = None, id: int = None):
        self.src = outlet
        self.dest = inlet
        self.id = id
        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name = "create_edge")

        if self.id is not None:
            if return_conn:
                action.commit = True
                action.execute()
            return

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

    