import weakref

from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action
from ResearchOS.idcreator import IDCreator
from ResearchOS.Bridges.input_types import Dynamic
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
        subset_id = self.input_dynamic.parent_ro.subset.id if self.input_dynamic.parent_ro.subset is not None else None
        return f"""{self.output_dynamic.parent_ro.id}: {self.output_dynamic.vr_name_in_code} -> {self.input_dynamic.parent_ro.id}: {self.input_dynamic.vr_name_in_code} Subset: {subset_id}"""
    

    def __init__(self, id: int = None,
                 action: Action = None,                  
                 input_dynamic: Dynamic = None,
                 output_dynamic: Dynamic = None,
                 inlet: Input = None,
                 outlet: Output = None,
                 print_edge: bool = False):
        
        if hasattr(self, "id"):
            return # Already initialized, loaded from Edge.instances
        
        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name = "create_edge")
        
        if id is not None:
            sqlquery_raw = "SELECT edge_id, source_let_put_id, target_let_put_id FROM pipelineobjects_graph WHERE edge_id = ?"
            sqlquery = sql_order_result(action, sqlquery_raw, ["edge_id"], single=True, user = True, computer = False)
            params = (id,)
            result = action.conn.execute(sqlquery, params).fetchall()
            if not result:
                raise ValueError(f"Edge with id {id} not found in database.")
            source_put_let_id, target_put_let_id = result[0][1], result[0][2]
            sqlquery_raw = "SELECT io_id, dynamic_vr_id, order_num, is_lookup FROM inputs_outputs_to_dynamic_vrs WHERE io_dynamic_id IN (?, ?)"
            sqlquery = sql_order_result(action, sqlquery_raw, [], single=False, user = True, computer = False)
            params = (source_put_let_id, target_put_let_id)
            result = action.conn.execute(sqlquery, params).fetchall()
            if not result:
                raise ValueError(f"Edge with id {id} not found in database.")
            # input_output_ids = [row[0] for row in result]
            dynamic_vr_ids = [row[1] for row in result]
            dynamic_vrs = [Dynamic(id=id, action=action) for id in dynamic_vr_ids]            
            
            inlet = [i for i in inlets_outlets if i.is_input][0]
            outlet = [i for i in inlets_outlets if not i.is_input][0]
            input_dynamic = dynamic_vrs[0] if inlet.is_input else dynamic_vrs[1]
            output_dynamic = dynamic_vrs[0] if outlet.is_input else dynamic_vrs[1]

        if input_dynamic.pr != output_dynamic.pr:
            raise ValueError("Input and output_dynamic must have the same Process (or Logsheet).")
        
        if input_dynamic.vr != output_dynamic.vr:
            raise ValueError("Input and output_dynamic must have the same Variable.")
                    
        self.id = id        

        if input_dynamic is not None:
            self.input_dynamic = input_dynamic
            self.inlet = inlet
            self.source_let_put_id = input_dynamic.id
        if output_dynamic is not None:
            self.output_dynamic = output_dynamic
            self.outlet = outlet
            self.target_let_put_id = output_dynamic.id

        self.source_pr = self.output_dynamic.pr
        self.target_pr = self.input_dynamic.pr
        self.vr = self.input_dynamic.vr
        self.is_lookup = self.input_dynamic.lookup_pr is not None

        if self.id is not None:
            return # Already loaded.        

        sqlquery_raw = "SELECT edge_id FROM pipelineobjects_graph WHERE source_let_put_id = ? AND target_let_put_id = ?"
        sqlquery = sql_order_result(action, sqlquery_raw, ["edge_id"], single=True, user = True, computer = False)
        params = (self.source_let_put_id, self.target_let_put_id)
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

    