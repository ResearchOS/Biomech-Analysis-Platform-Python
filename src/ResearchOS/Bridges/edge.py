import weakref

from ResearchOS.Bridges.inlet import Inlet
from ResearchOS.Bridges.outlet import Outlet
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action
from ResearchOS.idcreator import IDCreator

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
        return f"""{self.outlet.parent_ro.id} "{self.outlet.vr_name_in_code}" -> {self.inlet.parent_ro.id} "{self.inlet.vr_name_in_code}"."""
    
    @staticmethod
    def load(id: int, action: Action = None) -> "Edge":
        if id in Edge.instances.keys():
            return Edge.instances[id]        
        if action is None:            
            action = Action(name = f"load_edge")
        sqlquery_raw = "SELECT connection_id, outlet_id, inlet_id FROM connections WHERE connection_id = ?"
        sqlquery = sql_order_result(action, sqlquery_raw, ["connection_id"], single=True, user = True, computer = False)
        params = (id,)
        result = action.conn.cursor().execute(sqlquery, params).fetchall()
        if not result:
            raise ValueError(f"Edge with id {id} not found in database.")
        id, outlet_id, inlet_id = result[0]
        outlet = Outlet.load(outlet_id)
        inlet = Inlet.load(inlet_id)
        return Edge(outlet=outlet, inlet=inlet)
    
    def __init__(self, inlet: Inlet = None, outlet: Outlet = None, action: Action = None, id: int = None, print_edge: bool = False):
        self.outlet = outlet
        self.inlet = inlet
        self.id = id
        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name = "create_edge")

        sqlquery_raw = "SELECT connection_id FROM connections WHERE outlet_id = ? AND inlet_id = ? AND is_active = 1"
        sqlquery = sql_order_result(action, sqlquery_raw, ["outlet_id", "inlet_id"], single=True, user = True, computer = False)
        params = (outlet.id, inlet.id)
        result = action.conn.execute(sqlquery, params).fetchall()
        if result:
            self.id = result[0][0]
        else:
            sqlquery = "INSERT INTO connections (connection_id, outlet_id, inlet_id, action_id_num) VALUES (?, ?, ?, ?)"
            idcreator = IDCreator(action.conn)
            id = idcreator.create_generic_id("connections", "connection_id")
            params = (id, outlet.id, inlet.id, action.id_num)
            cursor = action.conn.cursor()
            cursor.execute(sqlquery, params)
            self.id = id

            sqlquery = "INSERT INTO pipelineobjects_graph (source_object_id, target_object_id, edge_id, action_id_num) VALUES (?, ?, ?, ?)"
            params = (outlet.parent_ro.id, inlet.parent_ro.id, self.id, action.id_num)
            cursor.execute(sqlquery, params)
            if print_edge:
                print("Created: ", self)

        if return_conn:
            action.commit = True
            action.execute()            

    