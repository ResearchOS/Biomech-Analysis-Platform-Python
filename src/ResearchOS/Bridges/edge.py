import weakref

from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action
from ResearchOS.idcreator import IDCreator
from ResearchOS.Bridges.input import Input
from ResearchOS.Bridges.output import Output

class Edge():

    instances = weakref.WeakValueDictionary()

    def __new__(cls, *args, **kwargs):
        id = None
        if "id" in kwargs.keys():
            id = kwargs["id"]                    
        if id in Edge.instances.keys():
            return Edge.instances[id]
        instance = super().__new__(cls)
        if id is not None:
            Edge.instances[id] = instance
        return instance

    def __str__(self):
        return f"""{self.output.parent_ro.id}: {self.output.vr_name_in_code} -> {self.input.parent_ro.id}: {self.input.vr_name_in_code} Subset: {self.input.parent_ro.subset.id}"""
    

    def __init__(self, id: int = None,
                 action: Action = None, 
                 print_edge: bool = False,
                 input_id: int = None,
                 output_id: int = None,
                 input: Input = None,
                 output: Output = None):
        
        if hasattr(self, "id"):
            return # Already initialized, loaded from Edge.instances
        
        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name = "create_edge")
        
        if id is not None:
            sqlquery_raw = "SELECT edge_id, input_id, output_id FROM pipelineobjects_graph WHERE edge_id = ?"
            sqlquery = sql_order_result(action, sqlquery_raw, ["edge_id"], single=True, user = True, computer = False)
            params = (id,)
            result = action.conn.execute(sqlquery, params).fetchall()
            if not result:
                raise ValueError(f"Edge with id {id} not found in database.")
            input = Input(id = result[0][1], action = action)
            output = Output(id = result[0][2], action = action)
                    
        self.id = id        

        if input is not None:
            self.input = input
            self.input_id = input.id
        else:
            self.input = Input(id = input_id, action = action)
            self.input_id = input_id
        if output is not None:
            self.output = output
            self.output_id = output.id
        else:
            self.output = Output(id = output_id, action = action)
            self.output_id = output_id

        if self.id is not None:
            return # Already loaded.

        sqlquery_raw = "SELECT edge_id FROM pipelineobjects_graph WHERE input_id = ? AND output_id = ?"
        sqlquery = sql_order_result(action, sqlquery_raw, ["edge_id"], single=True, user = True, computer = False)
        params = (self.input_id, self.output_id)
        result = action.conn.execute(sqlquery, params).fetchall()
        if result:
            self.id = result[0][0]
        else:
            idcreator = IDCreator(action.conn)
            id = idcreator.create_generic_id("pipelineobjects_graph", "edge_id")
            self.id = id
            params = (id, action.id_num, self.input_id, self.output_id)
            action.add_sql_query("None", "pipelineobjects_graph_insert", params)                        
            
            if print_edge:
                print("Created: ", self)

        if return_conn:
            action.commit = True
            action.execute()            

    