from typing import TYPE_CHECKING
import json

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

import networkx as nx

from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.Digraph.pipeline_digraph import PipelineDiGraph
from ResearchOS.cli.get_all_paths_of_type import import_objects_of_type
from ResearchOS.action import Action
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.variable import Variable




def make_all_edges(ro: "ResearchObject", action: Action = None, puts: dict = None, is_input: bool = True):
    """Make all the edges for a Research Object."""
    from ResearchOS.research_object import ResearchObject
    return_conn = False
    if action is None:
        action = Action(name="Build_PL")
        return_conn = True

    G = PipelineDiGraph()
    params_list = []
    subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
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

    # Check that there are no cycles being introduced.
    for param in params_list:
        # Main & lookup.
        source_id = param[3]
        target_id = param[1]        
        source_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix.startswith(source_id[0:2])][0] if source_id else None
        target_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix.startswith(target_id[0:2])][0]
        source = source_cls(id = source_id) if source_cls else None
        target = target_cls(id = target_id)
        if source_id == target_id:
            G.add_node(target, action=action)
            continue # This is an output. Edges only get created on inputs.
        if source_cls and target_cls:
            G.add_edge(source, target, vr = Variable(id=param[4]), tmp = False, action=action)

    if return_conn:
        action.commit = True
        action.execute()     