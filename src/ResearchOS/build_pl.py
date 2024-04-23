from typing import TYPE_CHECKING
import json

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

import networkx as nx

from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.cli.get_all_paths_of_type import import_objects_of_type
from ResearchOS.action import Action
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.variable import Variable

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
    from ResearchOS.PipelineObjects.plot import Plot
    from ResearchOS.PipelineObjects.stats import Stats
    # from src.research_objects import processes
    if import_objs: 
        import_objects_of_type(Process)
        # import_objects_of_type(Plot)
        # import_objects_of_type(Stats)

    return_conn = True
    if action is None:
        return_conn = False
        action = Action(name="Build_PL")
    sqlquery_raw = "SELECT edge_id, input_id, output_id FROM pipelineobjects_graph WHERE is_active = 1"
    sqlquery = sql_order_result(action, sqlquery_raw, ["edge_id"], single = True, user = True, computer = False)
    result = action.conn.cursor().execute(sqlquery).fetchall()
    if not result:
         raise ValueError("No connections found.")
    edges = [Edge(id = row[0], action=action) for row in result]

    G = nx.MultiDiGraph()
    for edge in edges:
        target_obj = edge.input.parent_ro
        source_obj = edge.output.pr
        G.add_edge(source_obj, target_obj, edge=edge)
        if edge.input.lookup_vr is not None:
            G.add_edge(edge.input.lookup_pr, target_obj, edge=edge)

    # import matplotlib.pyplot as plt

    # pos = nx.spring_layout(G)
    # nx.draw(G, pos, with_labels=True, node_size=700)

    # plt.show()

    if return_conn:
        action.commit = True
        action.execute()
    return G

def make_all_edges_from_dict(parent_ro: "ResearchObject", edges_dict: dict, action: Action = None):
    """Creates all the Edges from the given dictionary."""
    return_conn = False
    if action is None:
        return_conn = True
        action = Action(name="Build_PL")
    parent_ro_id = parent_ro.id        
    # For each VR name in the dictionary.
    for vr_name, vr_dict in edges_dict.items():        
        show = vr_dict["show"]
        main_vr = vr_dict["main"]["vr"]
        value = main_vr if not (isinstance(main_vr, Variable) or isinstance(main_vr, str) and main_vr.startswith("VR")) else None
        vr_id = None
        if not value:
            vr_id = main_vr.id if isinstance(main_vr, Variable) else main_vr
        pr_ids = vr_dict["main"]["pr"] if vr_id else []
        if pr_ids and not isinstance(pr_ids, list):
            pr_ids = [pr_ids]        
        lookup_vr_id = vr_dict["lookup"]["vr"]
        lookup_vr_id = lookup_vr_id.id if isinstance(lookup_vr_id, Variable) else lookup_vr_id

        lookup_pr_ids = vr_dict["lookup"]["pr"] if lookup_vr_id else []
        if lookup_pr_ids and not isinstance(lookup_pr_ids, list):
            lookup_pr_ids = [lookup_pr_ids]
        # For each PR in the pr_ids.
        order_num = -1
        for pr_id in pr_ids:
            order_num += 1
            params = (action.id_num, parent_ro_id, vr_name, pr_id, vr_id, value, order_num, False, show, 1)
            if not action.is_redundant_params(None, "pipeline_insert", params):
                action.add_sql_query(None, "pipeline_insert", params)        
        order_num = -1
        for lookup_pr_id in lookup_pr_ids:
            order_num += 1
            params = (action.id_num, parent_ro_id, vr_name, lookup_pr_id, lookup_vr_id, None, order_num, True, show, 1)
            if not action.is_redundant_params(None, "pipeline_insert", params):
                action.add_sql_query(None, "pipeline_insert", params)

    if return_conn:
        action.commit = True
        action.execute()

        




def make_all_edges(ro: "ResearchObject", action: Action = None, puts: dict = None):
        # For each input, find an Outlet with a matching output.
        from ResearchOS.PipelineObjects.process import Process
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        return_conn = False
        if action is None:
            action = Action(name="Build_PL")
            return_conn = True

        params_list = []
        for key, input in puts.items():
            show = input["show"]
            main_vr = input["main"]["vr"]
            value = main_vr if not (isinstance(main_vr, Variable) or isinstance(main_vr, str) and main_vr.startswith("VR")) else None
            vr_id = None
            if not value:
                vr_id = main_vr.id if isinstance(main_vr, Variable) else main_vr
            pr_ids = input["main"]["pr"] if vr_id else []
            if pr_ids and not isinstance(pr_ids, list):
                pr_ids = [pr_ids]
            lookup_vr_id = input["lookup"]["vr"]
            lookup_vr_id = lookup_vr_id.id if isinstance(lookup_vr_id, Variable) else lookup_vr_id
            lookup_pr_ids = input["lookup"]["pr"] if lookup_vr_id else []
            # Hard-coded value.
            if value:
                params = (action.id_num, ro.id, key, None, None, json.dumps(value), 0, int(False), int(show), int(True))
                if not action.is_redundant_params(None, "pipeline_insert", params):
                    params_list.append(params)
            elif vr_id:
                # Import file VR name.
                if hasattr(ro, "import_file_vr_name") and key == ro.import_file_vr_name:
                    params = (action.id_num, ro.id, key, None, None, None, 0, int(False), int(show), int(True))
                    if not action.is_redundant_params(None, "pipeline_insert", params):
                        params_list.append(params)                
            for order_num, pr in enumerate(pr_ids):
                params = (action.id_num, ro.id, key, pr, vr_id, value, order_num, int(False), int(show), int(True))
                if not action.is_redundant_params(None, "pipeline_insert", params):
                    params_list.append(params)
            for order_num, pr in enumerate(lookup_pr_ids):
                params = (action.id_num, ro.id, key, pr, lookup_vr_id, None, order_num, int(True), int(show), int(True))
                if not action.is_redundant_params(None, "pipeline_insert", params):
                    params_list.append(params)

        # Check for redundancy in the database.
        sql_params = [param[1:] for param in params_list] # Remove the action_id_num from the params.
        params_str = "(parent_ro_id IS ? AND vr_name_in_code IS ? AND source_pr_id IS ? AND vr_id IS ? AND hard_coded_value IS ? AND order_num IS ? AND is_lookup IS ? AND show IS ? AND is_active IS ?)"
        sqlquery = "SELECT parent_ro_id, vr_name_in_code, source_pr_id, vr_id, hard_coded_value, order_num, is_lookup, show, is_active FROM pipeline WHERE {}".format(" OR ".join([params_str for params in sql_params]))
        all_params = []
        for param in sql_params:
            all_params.extend(param)
        result = action.conn.cursor().execute(sqlquery, all_params).fetchall()        
        remove_params = []
        for param in params_list:
            if param[1:] in result:
                remove_params.append(param)

        for param in remove_params:
            params_list.remove(param)

        for params in params_list:
            action.add_sql_query(None, "pipeline_insert", params)
        
        action.commit = True
        action.execute()

if __name__=="__main__":
    import sys, os
    sys.path.append(os.getcwd())
    sys.path.append(os.getcwd() + "/src")
    build_pl()
    print("Done.")