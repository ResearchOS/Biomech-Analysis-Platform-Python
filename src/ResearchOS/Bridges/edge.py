# from typing import TYPE_CHECKING

# if TYPE_CHECKING:
#     from ResearchOS.research_object import ResearchObject

# from ResearchOS.sql.sql_runner import sql_order_result
# from ResearchOS.idcreator import IDCreator
# from ResearchOS.Bridges.input_types import Dynamic
# from ResearchOS.Bridges.input import Input
# from ResearchOS.Bridges.output import Output
from ResearchOS.action import Action
from ResearchOS.Bridges.pipeline_parts import PipelineParts
from ResearchOS.Bridges.letput import LetPut

class Edge(PipelineParts):

    cls_name = "Edge"
    table_name = "pipelineobjects_graph"
    id_col = "edge_id"
    col_names = ["source_let_put_id", "target_let_put_id"]
    insert_query_name = "pipelineobjects_graph_insert"

    def __init__(self, id: int = None,
                    action: Action = None,
                    source_let_put_id: str = None,
                    target_let_put_id: str = None):
        self.source_let_put_id = source_let_put_id
        self.target_let_put_id = target_let_put_id
        super().__init__(id = id, action = action)    
        

    def __str__(self):
        subset_id = self.input_dynamic.parent_ro.subset.id if self.input_dynamic.parent_ro.subset is not None else None
        return f"""{self.output_dynamic.parent_ro.id}: {self.output_dynamic.vr_name_in_code} -> {self.input_dynamic.parent_ro.id}: {self.input_dynamic.vr_name_in_code} Subset: {subset_id}"""
    

    def load_from_db(self, source_let_put_id: str, target_let_put_id: str):
        """Load the let_put objects from the database."""
        source_let_put = LetPut(id = source_let_put_id, action = self.action)
        target_let_put = LetPut(id = target_let_put_id, action = self.action)
        self.source_let_put = source_let_put
        self.target_let_put = target_let_put


    # def __init__(self, id: int = None,
    #              action: Action = None,
    #              source_let_put_id: str = None,
    #              input: Input = None,
    #              output: Output = None,
    #              print_edge: bool = False):
        
    #     if hasattr(self, "id"):
    #         return # Already initialized, loaded from Edge.instances                
        
    #     return_conn = False
    #     if action is None:
    #         return_conn = True
    #         action = Action(name = "create_edge")
        
    #     if id is not None:
    #         sqlquery_raw = "SELECT edge_id, source_let_put_id, target_let_put_id FROM pipelineobjects_graph WHERE edge_id = ?"
    #         sqlquery = sql_order_result(action, sqlquery_raw, ["edge_id"], single=True, user = True, computer = False)
    #         params = (id,)
    #         result = action.conn.execute(sqlquery, params).fetchall()
    #         if not result:
    #             raise ValueError(f"Edge with id {id} not found in database.")
    #         source_put_let_id, target_put_let_id = result[0][1], result[0][2]
    #         sqlquery_raw = "SELECT io_id, dynamic_vr_id, order_num, is_lookup FROM lets_puts WHERE io_dynamic_id IN (?, ?)"
    #         sqlquery = sql_order_result(action, sqlquery_raw, [], single=False, user = True, computer = False)
    #         params = (source_put_let_id, target_put_let_id)
    #         result = action.conn.execute(sqlquery, params).fetchall()
    #         if not result:
    #             raise ValueError(f"Edge with id {id} not found in database.")
    #         input_output_ids = [row[0] for row in result]
    #         # try:
    #         #     input = Input(id=input_output_ids[0], action=action)
    #         # except:
    #         #     input = Input(id=input_output_ids[1], action=action)
    #         # try:
    #         #     output = Output(id=input_output_ids[0], action=action)
    #         # except:
    #         #     output = Output(id=input_output_ids[1], action=action)         
            
    #         inlet = [i for i in inlets_outlets if i.is_input][0]
    #         outlet = [i for i in inlets_outlets if not i.is_input][0]
    #         input_dynamic = dynamic_vrs[0] if inlet.is_input else dynamic_vrs[1]
    #         output_dynamic = dynamic_vrs[0] if outlet.is_input else dynamic_vrs[1]
    #     else:
    #         # Prep for creating a new edge. Need to build towards input & output let_put_ids, given a few different options.
    #         if input_dynamic is not None and inlet is not None:
    #             source_vr_name_in_code = None
    #             source_research_object = None
    #             source_vr = None
    #             source_pr = None
    #         else:
    #             # Get the VR & PR info from the inlet object.
    #             input_dynamic = Dynamic(vr = source_vr, pr = source_pr, is_input = True, action = action)
    #             inlet = Inlet(ro_id = source_research_object.id, vr_name_in_code = source_vr_name_in_code, action = action)
    #         if output_dynamic is not None and outlet is not None:
    #             target_vr_name_in_code = None
    #             target_research_object = None
    #             target_vr = None
    #             target_pr = None
    #         else:
    #             # Get the VR & PR info from the outlet object.
    #             output_dynamic = Dynamic(vr = target_vr, pr = target_pr, is_input = False, action = action)
    #             outlet = Outlet(ro_id = target_research_object.id, vr_name_in_code = target_vr_name_in_code, action = action)

    #     if input_dynamic.pr != output_dynamic.pr:
    #         raise ValueError("Input and output_dynamic must have the same Process (or Logsheet).")
        
    #     if input_dynamic.vr != output_dynamic.vr:
    #         raise ValueError("Input and output_dynamic must have the same Variable.")
        
    #     if outlet.parent_ro != output_dynamic.pr or inlet.parent_ro != input_dynamic.pr:
    #         raise ValueError("Inlet and Outlet must have the same Process (or Logsheet) as the Input & Output Dynamic object.")
                    
    #     self.id = id        

    #     if input_dynamic is not None:
    #         self.input_dynamic = input_dynamic
    #         self.inlet = inlet
    #         self.source_let_put_id = input_dynamic.id
    #     if output_dynamic is not None:
    #         self.output_dynamic = output_dynamic
    #         self.outlet = outlet
    #         self.target_let_put_id = output_dynamic.id

    #     self.source_pr = self.output_dynamic.pr
    #     self.target_pr = self.input_dynamic.pr
    #     self.vr = self.input_dynamic.vr
    #     self.is_lookup = self.input_dynamic.lookup_pr is not None

    #     if self.id is not None:
    #         return # Already loaded.        

    #     sqlquery_raw = "SELECT edge_id FROM pipelineobjects_graph WHERE source_let_put_id = ? AND target_let_put_id = ?"
    #     sqlquery = sql_order_result(action, sqlquery_raw, ["edge_id"], single=True, user = True, computer = False)
    #     params = (self.source_let_put_id, self.target_let_put_id)
    #     result = action.conn.execute(sqlquery, params).fetchall()
    #     if result:
    #         self.id = result[0][0]
    #     else:
    #         idcreator = IDCreator(action.conn)
    #         id = idcreator.create_generic_id("pipelineobjects_graph", "edge_id")
    #         self.id = id
    #         params = (id, action.id_num, self.input_id, self.output_id)
    #         action.add_sql_query("None", "pipelineobjects_graph_insert", params)                        
            
    #         if print_edge:
    #             print("Created: ", self)

    #     if return_conn:
    #         action.commit = True
    #         action.execute()            

    