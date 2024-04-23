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
    """Builds the pipeline from the objects in the code."""   
    from ResearchOS.research_object import ResearchObject
    from ResearchOS.default_attrs import DefaultAttrs
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.plot import Plot
    from ResearchOS.PipelineObjects.stats import Stats
    from src.research_objects import plots
    return_conn = True
    if action is None:
        return_conn = False
        action = Action(name="Build_PL")
    if import_objs: 
        prs, pr_mods = import_objects_of_type(Process)
        pls, pl_mods = import_objects_of_type(Plot)
        sts, st_mods = import_objects_of_type(Stats)
        if action is None:
            return_conn = False
            action = Action(name="Build_PL")
        
        def set_names(objs, mods, ttype, action):
            if not objs or not mods:
                print(f"No objects found of type {ttype.__name__}.")
                return
            default_attrs = DefaultAttrs(objs[0]).default_attrs if prs else {}
            for mod in mods:
                for obj_name in dir(mod):
                    obj = getattr(mod, obj_name)
                    if not isinstance(obj, ttype):
                        continue
                    if obj.name==obj.id or obj.name==default_attrs["name"]:
                        obj.__setattr__("name", obj_name, action=action, exec=False)
        set_names(prs, pr_mods, Process, action=action)
        set_names(pls, pl_mods, Plot, action=action)
        set_names(sts, st_mods, Stats, action=action)
    
    sqlquery_raw = "SELECT parent_ro_id, vr_name_in_code, source_pr_id, vr_id FROM pipeline WHERE is_active = 1 AND source_pr_id IS NOT pipeline.parent_ro_id AND source_pr_id IS NOT NULL"
    sqlquery = sql_order_result(action, sqlquery_raw, ["edge_id"], single = True, user = True, computer = False)
    result = action.conn.cursor().execute(sqlquery).fetchall()
    if not result:
         raise ValueError("No connections found.")

    G = nx.MultiDiGraph()
    subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
    for row in result:
        parent_ro_id = row[0]
        vr_name_in_code = row[1]
        source_pr_id = row[2]
        vr_id = row[3]
        parent_ro_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and parent_ro_id.startswith(cls.prefix)][0]
        source_pr_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and source_pr_id.startswith(cls.prefix)][0]
        parent_ro = parent_ro_cls(id = parent_ro_id)
        source_pr = source_pr_cls(id = source_pr_id)
        vr = Variable(id = vr_id)
        G.add_edge(source_pr, parent_ro, vr_name_in_code = vr_name_in_code, vr = vr)
        if not nx.is_directed_acyclic_graph(G):
            raise ValueError("The pipeline is not a directed acyclic graph.")

    import matplotlib.pyplot as plt

    pos = nx.spring_layout(G)
    nx.draw(G, pos, with_labels=True, node_size=700)
    edge_labels = nx.get_edge_attributes(G, 'vr_name_in_code')
    nx.draw_networkx_edge_labels(nx.DiGraph(G), pos, edge_labels=edge_labels)

    plt.show()

    if return_conn:
        action.commit = True
        action.execute()
    return G


def make_all_edges(ro: "ResearchObject", action: Action = None, puts: dict = None, is_input: bool = True):
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

        # Check if there are any edges that have been removed. These would be vr_name_in_code's for this parent_ro_id that are not in the puts.
        sqlquery_raw = "SELECT vr_name_in_code FROM pipeline WHERE parent_ro_id = ? AND is_active = 1 AND"
        if is_input:
            operator = "IS NOT"
        else:
            operator = "IS"
        sqlquery_raw += " source_pr_id " + operator + " pipeline.parent_ro_id" # Need to include the "pipeline." prefix because of the way sql_order_result works.
        sqlquery = sql_order_result(action, sqlquery_raw, ["vr_name_in_code"], single = True, user = True, computer = False)
        params = (ro.id,)
        result = action.conn.cursor().execute(sqlquery, params).fetchall()
        vr_names = [row[0] for row in result] if result else []
        for vr_name in vr_names:
            if vr_name not in puts.keys():                
                params = (action.id_num, ro.id, vr_name, None, None, None, 0, int(False), int(False), int(False)) # Some default settings.
                action.add_sql_query(None, "pipeline_insert", params)

        # Check for redundancy in the database before writing.
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