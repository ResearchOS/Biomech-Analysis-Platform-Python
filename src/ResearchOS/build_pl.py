from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

import networkx as nx

from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.cli.get_all_paths_of_type import import_objects_of_type
from ResearchOS.Bridges.edge import Edge
from ResearchOS.action import Action
from ResearchOS.sql.sql_runner import sql_order_result

# 1. When running a Process's "set_inputs" or "set_outputs" the user will expect to have the inputs & outputs connected to the proper places after each run.
# However, when "building" the pipeline, doing that for each Process would be inefficient.
# Therefore, I am including a separate "build_pl" function that will be called after all the Processes have been created.
# This function will connect all the inputs and outputs of the Processes to the proper places.

# "Multi" mode.
# While the Inlets & Outlets and Inputs & Outputs are created in SQL when the Processes' settings are created, the Edges are created in SQL when the pipeline is built.
# This is because the Edges need to see all of the available Inlets & Outlets before they can be created.

# "Single" (non-Multi) mode.
# The Inlets & Outlets and Inputs & Outputs are created in SQL when the Processes' settings are created.
# Edges are created in SQL when the Processes' settings are created, using currently available Processes in memory.

def build_pl(import_objs: bool = True, action: Action = None) -> nx.MultiDiGraph:
    """Builds the pipeline."""   
    from ResearchOS.PipelineObjects.process import Process
    from src.research_objects import processes
    # if import_objs: 
    #     import_objects_of_type(Process)

    return_conn = True
    if action is None:
        return_conn = False
        action = Action(name="Build_PL")
    sqlquery_raw = "SELECT source_object_id, target_object_id, edge_id FROM pipelineobjects_graph WHERE is_active = 1"
    sqlquery = sql_order_result(action, sqlquery_raw, ["source_object_id", "target_object_id", "edge_id"], single = True, user = True, computer = False)
    result = action.conn.cursor().execute(sqlquery).fetchall()
    if not result:
         raise ValueError("No connections found.")
    edges = [Edge.load(id = row[2], action=action) for row in result]

    G = nx.MultiDiGraph()
    for edge in edges:
        target_obj = edge.inlet.parent_ro
        source_obj = edge.outlet.parent_ro
        G.add_edge(source_obj, target_obj, edge=edge)

    if return_conn:
        action.commit = True
        action.execute()
    return G

def make_all_edges(ro: "ResearchObject"):
        # For each input, find an Outlet with a matching output.
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        from ResearchOS.Bridges.input_types import DynamicMain
        from ResearchOS.Bridges.output import Output
        all_pr_objs = [pr() for pr in ResearchObjectHandler.instances_list if pr() is not None and isinstance(pr(), (Process,))]
        lg_objs = [lg() for lg in ResearchObjectHandler.instances_list if lg() is not None and isinstance(lg(), (Logsheet,))]
        last_idx = all_pr_objs.index(ro)
        all_pr_objs = all_pr_objs[:last_idx]
        action = Action(name="Build_PL")
        for key, input in ro.inputs.items():            
            if not isinstance(input.put_value, DynamicMain):
                continue
            for pr in all_pr_objs:
                for output in pr.outputs.values():                    
                    if output.vr is None:
                         continue
                    if input.vr == output.vr and input.pr == output.pr:
                        e = Edge(input=input, output=output, action=action, print_edge=True)                        
            for lg in lg_objs:
                 lg.validate_headers(lg.headers, action, [])
                 for h in lg.headers:
                      if h[3] == input.vr:
                        output = Output(vr=input.vr, pr=lg, action=action, show=True, parent_ro=lg, vr_name_in_code=h[0])
                        e = Edge(input=input, output=output, action=action, print_edge=True)  
                        break                      
        
        # Now that all the Edges have been created, commit the Action.
        action.commit = True
        action.execute()

if __name__=="__main__":
    import sys, os
    sys.path.append(os.getcwd())
    sys.path.append(os.getcwd() + "/src")
    build_pl()
    print("Done.")