from typing import TYPE_CHECKING, Any
import json

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

import networkx as nx
from ResearchOS.action import Action
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.variable import Variable
from ResearchOS.cli.get_all_paths_of_type import import_objects_of_type

# class PipelineDiGraphMeta(type):
#     """Metaclass for the PipelineDiGraph class."""
#     def __call__(cls, *args, **kwargs):
#         obj = PipelineDiGraph.G
#         if obj is None:
#             obj = cls.__new__(cls, *args, **kwargs)
#         return obj

# metaclass=PipelineDiGraphMeta
class PipelineDiGraph(): 

    G: nx.MultiDiGraph = None # Cache for the DiGraph.

    # def __new__(cls, *args, **kwargs):
    #     """Create a new Pipeline DiGraph."""
    #     return super(PipelineDiGraph, cls).__new__(cls)

    def __init__(self, action: Action = None):
        """Initialize the Pipeline DiGraph.
        Be sure to consider the case of a PR object whose output VR's are not used as inputs anywhere?"""
        # 1. Ensure it is a MultiDiGraph.
        self.build_pl_from_db(action = action)

    def add_node(self, node_for_adding: "ResearchObject", 
                 vr_name_in_code: str = None,
                 action: Action = None, 
                 is_input: bool = False):
        """Add a node to the Pipeline DiGraph.
        By providing a vr_name_in_code, the Output VR is set for the node."""
        G = self.G
        if not vr_name_in_code:
            raise ValueError("No VR name in code provided.")
        if not G.has_node(node_for_adding):
            G.add_node(node_for_adding)
        if "inputs" not in G.nodes[node_for_adding]:
            G.nodes[node_for_adding]["inputs"] = {}
        if "outputs" not in G.nodes[node_for_adding]:
            G.nodes[node_for_adding]["outputs"] = {}

        # Check for duplicates.
        if is_input:
            graph_puts = G.nodes[node_for_adding]["inputs"]
            puts = node_for_adding.inputs
        else:
            graph_puts = G.nodes[node_for_adding]["outputs"]
            puts = node_for_adding.outputs

        vr = puts[vr_name_in_code]["main"]["vr"]
        if isinstance(vr, str) and vr.startswith(Variable.prefix):
            vr = Variable(id=vr, action=action)

        if vr_name_in_code in graph_puts and graph_puts[vr_name_in_code] == vr:
            return
        
        graph_puts[vr_name_in_code] = vr # Continue on to add the node to the database.
        
        # Add to the database.
        # (row_id, action_id_num, ro_id, vr_name_in_code, vr_id, pr_ids, lookup_vr_id, lookup_pr_ids, hard_coded_value, is_input, show, is_active)        
        show = True
        ro_id = node_for_adding.id             
        vr_id = vr.id if isinstance(vr, Variable) else None
        vr_slice = puts[vr_name_in_code]["slice"]
        vr_slice = json.dumps(vr_slice) if vr_slice is not None else None
        is_active = 1
        pr_ids = json.dumps(puts[vr_name_in_code]["main"]["pr"])
        lookup_vr_id = puts[vr_name_in_code]["lookup"]["vr"] if not isinstance(puts[vr_name_in_code]["lookup"]["vr"], Variable) else puts[vr_name_in_code]["lookup"]["vr"].id
        lookup_pr_ids = json.dumps(puts[vr_name_in_code]["lookup"]["pr"])
        if not isinstance(vr, Variable):
            value = puts[vr_name_in_code]["main"]["vr"] # Hard-coded
            value = json.dumps(value) if value is not None else None
        else:
            value = vr.hard_coded_value if vr and vr.hard_coded_value else None
        params = (action.id_num, ro_id, vr_name_in_code, vr_id, vr_slice, pr_ids, lookup_vr_id, lookup_pr_ids, value, is_input, int(show), is_active)
        action.add_sql_query(None, "node_insert", params)

    def add_edge(self, source_object: "ResearchObject", target_object: "ResearchObject", vr: "Variable", tmp: bool = False, action: Action = None):
        """Add an edge to the Pipeline DiGraph.
        source_object and target_object must both exist. The inputs & outputs must be fully formed."""
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        # Check if there's a VR.
        if not vr:
            raise ValueError("No VR provided.")
        # For inputs & outputs, not edges.
        if not target_object or not source_object:
            raise ValueError("Both source and target objects must exist.")
        # Check for duplicate edges.
        G = PipelineDiGraph.G
        if G.has_edge(source_object, target_object, key = vr):
            return
        
        # Add the edge to the DiGraph.
        G.add_edge(source_object, target_object, key = vr)
        if not nx.is_directed_acyclic_graph(G):
            G.remove_edge(source_object, target_object, key = vr)
            raise ValueError("The edge would create a cycle!")
            # return            
        
        if tmp:
            G.remove_edge(source_object, target_object, key = vr)
            return
        
        # Convert the edge to a database entry.
        target_pr_id = target_object.id
        source_pr_id = source_object.id
                
        vr_id = vr.id if isinstance(vr, Variable) else Variable(id=vr, action=action).id
        params = (action.id_num, source_pr_id, target_pr_id, vr_id, 1)
        action.add_sql_query(None, "edge_insert", params)
            

    def build_pl_from_db(self, action: Action = None):
        """Reads the Pipeline DiGraph from the database."""
        from ResearchOS.research_object import ResearchObject
        subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
        if PipelineDiGraph.G:
            return
        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name="Build_PL")

        G = nx.MultiDiGraph()

        # Get all nodes
        sqlquery_raw = "SELECT ro_id, vr_name_in_code, is_input, vr_id, hard_coded_value, vr_slice FROM nodes WHERE is_active = 1"
        sqlquery = sql_order_result(action, sqlquery_raw, ["ro_id", "vr_name_in_code"], single = True, user = True, computer = False)
        result = action.conn.cursor().execute(sqlquery).fetchall()
        if not result:
            PipelineDiGraph.G = G
            return
        
        # Add nodes to graph.
        for r in result:
            cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix.startswith(r[0][0:2])][0]
            ro = cls(id = r[0], action=action)
            vr_name_in_code = r[1]
            is_input = bool(r[2])
            vr_slice = json.loads(r[5]) if r[5] else None
            full_slice = []
            if vr_slice is not None:
                for slice_elem in vr_slice:
                    if isinstance(slice_elem, list):
                        start = slice_elem[0] if slice_elem[0] != "None" else None
                        stop = slice_elem[1] if slice_elem[1] != "None" else None
                        step = slice_elem[2] if slice_elem[2] != "None" else None
                        slice_elem = slice(start, stop, step)
                    full_slice.append(slice_elem)
            full_slice = tuple(full_slice) if full_slice else None
            if not G.has_node(ro):
                G.add_node(ro)
                G.nodes[ro]["inputs"] = {}
                G.nodes[ro]["outputs"] = {}
            if is_input:
                graph_puts = G.nodes[ro]["inputs"]
            else:
                graph_puts = G.nodes[ro]["outputs"]
            if vr_name_in_code not in graph_puts:
                hard_coded_value = json.loads(r[4]) if r[4] else None
                vr = Variable(id=r[3], action=action) if isinstance(r[3], str) and r[3].startswith(Variable.prefix) else None
                if vr and not vr._slice:
                    vr._slice = full_slice # Only when it's being loaded from the database.
                value = vr if not hard_coded_value else hard_coded_value
                graph_puts[vr_name_in_code] = value

        # Add nodes & edges to the DiGraph.        
        sqlquery_raw = "SELECT source_pr_id, target_pr_id, vr_id FROM edges WHERE is_active = 1"
        sqlquery = sql_order_result(action, sqlquery_raw, ["source_pr_id", "target_pr_id", "vr_id"], single = True, user = True, computer = False)
        result = action.conn.cursor().execute(sqlquery).fetchall()
        for row in result:
            # Row params to source & target & VR objects.            
            source_pr_id = row[0]
            target_pr_id = row[1]            
            vr_id = row[2]
            target_pr_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and target_pr_id.startswith(cls.prefix)][0]
            target_pr = target_pr_cls(id = target_pr_id, action=action)
            source_pr_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and source_pr_id.startswith(cls.prefix)][0]            
            source_pr = source_pr_cls(id = source_pr_id, action=action)
            vr = Variable(id = vr_id, action=action) if vr_id else None
            G.add_edge(source_pr, target_pr, key = vr, action=action)     

        PipelineDiGraph.G = G

        if return_conn:
            action.commit = True
            action.execute()

    def show(self):
        """Show the Pipeline DiGraph."""
        import matplotlib.pyplot as plt

        pos = nx.spring_layout(self)
        nx.draw(self, pos, with_labels=True, node_size=700)
        # edge_labels = nx.get_edge_attributes(self, 'vr_name_in_code')
        # nx.draw_networkx_edge_labels(nx.DiGraph(self), pos, edge_labels=edge_labels)

        plt.show()

def import_pl_objs(action: Action = None) -> nx.MultiDiGraph:
    """Builds the pipeline from the objects in the code."""
    from ResearchOS.default_attrs import DefaultAttrs
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.plot import Plot
    from ResearchOS.PipelineObjects.stats import Stats
    from src.research_objects import plots   
    from src.research_objects import processes 
    # from src.research_objects import stats
    return_conn = True
    if action is None:
        return_conn = False
        action = Action(name="Build_PL")
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

    if return_conn:
        action.commit = True
        action.execute()

def write_puts_dict_to_db(ro: "ResearchObject", action: Action = None, puts: dict = None, is_input: bool = True, prev_puts: dict = None):
    """Write the inputs or outputs of a ResearchObject to the database. Also makes edges & nodes in graph.
    "puts" is the dict of inputs or outputs for that ResearchObject.
    Nodes: (row_id, action_id_num, ro_id, vr_name_in_code, vr_id, pr_ids, lookup_vr_id, lookup_pr_ids, hard_coded_value, is_input, show, is_active)
    Edges: (edge_id, action_id_num, source_pr_id, target_pr_id, vr_id, is_active)"""
    from ResearchOS.research_object import ResearchObject
    return_conn = False
    if action is None:
        action = Action(name="Build_PL")
        return_conn = True

    graph = PipelineDiGraph(action=action)
    G = graph.G
    nodes_params_list, edges_params_list = puts_dict_to_params_list(puts, action, ro, is_input)
    subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)

    # Check if there are any nodes that have been removed. These would be vr_name_in_code's for this pr_id that are not in the puts.
    rem_vr_names = [vr_name for vr_name in prev_puts.keys() if vr_name not in puts.keys()] if puts else []    
    for vr_name in rem_vr_names:
        empty_list_json = json.dumps([])
        vr_id = None
        vr_slice = None
        pr_ids = empty_list_json
        lookup_vr_id = None
        lookup_pr_ids = empty_list_json
        value = None
        params = (action.id_num, ro.id, vr_name, vr_id, vr_slice, pr_ids, lookup_vr_id, lookup_pr_ids, value, int(is_input), int(False), int(False)) # Some default settings.
        action.add_sql_query(None, "node_insert", params)

        # Check if there are any edges to be removed.
        vr_id = prev_puts[vr_name]["main"]["vr"]
        if isinstance(vr_id, str) and vr_id.startswith(Variable.prefix):
            vr = Variable(id=vr_id, action=action)
            vr_id = vr.id
        elif isinstance(vr_id, Variable):
            vr_id = vr_id.id # This shouldn't be necessary, but why not just in case?
        rem_edges = [e for e in G.edges if (e[1]==ro.id) and e[2]==vr_id]
        # Possible that the rem_edges is empty, if the input VR is hard-coded or otherwise does not have an edge.
        for edge in rem_edges:
            G.remove_edge(edge[0], edge[1], key = edge[2])
            edge_param = (action.id_num, edge[0].id, edge[1].id, edge[2].id, int(False))
            action.add_sql_query(None, "edge_insert", edge_param)

    # Convert the params lists to inputs for G.add_node and G.add_edge.
    add_edges_list = []
    for param in edges_params_list:        
        # Main & lookup.
        source_id = param[1]
        target_id = param[2]        
        vr_id = param[3]
        source_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix.startswith(source_id[0:2])][0] if source_id else None
        target_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix.startswith(target_id[0:2])][0] if target_id else None
        source = source_cls(id = source_id, action=action)
        target = target_cls(id = target_id, action=action)
        vr = Variable(id=vr_id, action=action)
        if not vr_id and (hasattr(ro, "import_file_vr_name") and ro.import_file_vr_name is not None):
            vr_name_in_code = ro.import_file_vr_name
            vr = Variable(id=ro.inputs[vr_name_in_code]["main"]["vr"], action=action)
        # Performs the cycle check and adds the query to the action.
        add_edges_list.append((source, target, vr))    

    # Add the nodes to the database.
    add_nodes_list = []
    for node_params in nodes_params_list:
        node_id = node_params[1]
        vr_name_in_code = node_params[2]
        cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix.startswith(node_id[0:2])][0]
        ro = cls(id=node_id, action=action)
        # Adds the query to the action.
        add_nodes_list.append((ro, vr_name_in_code))
    
    # Actually add the nodes & edges.
    for ro, vr_name_in_code in add_nodes_list:
        graph.add_node(ro, vr_name_in_code=vr_name_in_code, is_input=is_input, action=action)

    for source, target, vr in add_edges_list:
        graph.add_edge(source, target, vr=vr, action=action)    

    if return_conn:
        action.commit = True
        action.execute()


def puts_dict_to_params_list(puts: dict, action: Action = None, ro: "ResearchObject" = None, is_input: bool = True) -> list:
    """Convert the JSON-serializable dict of inputs or outputs to a list of params tuples for the database.
    Does NOT check for duplicate edges/nodes being added that are already present in the graph. That's done when assigning to the database.
    (row_id, action_id_num, ro_id, vr_name_in_code, vr_id, pr_ids, lookup_vr_id, lookup_pr_ids, hard_coded_value, is_input, show, is_active)"""
    nodes_params_list = []    
    edges_params_list = []
    for key, put in puts.items():   
        slice = json.dumps(put["slice"]) if put["slice"] else None
        show = put["show"]
        main_vr = put["main"]["vr"]
        if isinstance(main_vr, Variable) or (isinstance(main_vr, str) and main_vr.startswith("VR")):
            value = None
        else:
            value = main_vr        
        vr_id = None
        if not value:
            vr_id = main_vr.id if isinstance(main_vr, Variable) else main_vr
        pr_ids = put["main"]["pr"] if vr_id else []
        lookup_vr_id = put["lookup"]["vr"]
        lookup_vr_id = lookup_vr_id.id if isinstance(lookup_vr_id, Variable) else lookup_vr_id
        lookup_pr_ids = put["lookup"]["pr"] if lookup_vr_id else []
        # Hard-coded value.
        if not vr_id:
            value = json.dumps(value) if value is not None else None
            params = (action.id_num, ro.id, key, None, None, None, None, None, value, int(is_input), int(show), int(True))
            nodes_params_list.append(params)
            continue
        elif vr_id:
            # Import file VR name.
            if hasattr(ro, "import_file_vr_name") and key == ro.import_file_vr_name:
                params = (action.id_num, ro.id, key, None, None, None, None, None, 0, int(is_input), int(show), int(True))
                nodes_params_list.append(params)    
                continue

        # Edges & dynamic nodes, if any.      
        value = value if not value else json.dumps(value) 
        node_params = (action.id_num, ro.id, key, vr_id, slice, json.dumps(pr_ids), lookup_vr_id, json.dumps(lookup_pr_ids), value, int(is_input), int(show), int(True))
        nodes_params_list.append(node_params)
        for pr in pr_ids:
            assert is_input                        
            edge_params = (action.id_num, pr, ro.id, vr_id)            
            edges_params_list.append(edge_params)
        for pr in lookup_pr_ids:
            assert is_input
            edge_params = (action.id_num, pr, ro.id, lookup_vr_id)
            edges_params_list.append(edge_params)

    edges_params_list = list(set(edges_params_list))
    nodes_params_list = list(set(nodes_params_list))

    return nodes_params_list, edges_params_list