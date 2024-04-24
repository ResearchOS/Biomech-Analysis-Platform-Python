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

class PipelineDiGraphMeta(type):
    """Metaclass for the PipelineDiGraph class."""
    def __call__(cls, *args, **kwargs):
        force_init = getattr(kwargs, "force_init", False)
        obj, found_in_cache = cls.__new__(cls, *args, **kwargs)
        if not found_in_cache or force_init:
            obj.__init__(*args, **kwargs)
        return obj

class PipelineDiGraph(nx.MultiDiGraph, metaclass=PipelineDiGraphMeta): 

    G: nx.MultiDiGraph = None # Cache for the DiGraph.

    def __new__(cls, *args, **kwargs):
        """Create a new Pipeline DiGraph."""
        if PipelineDiGraph.G is None:
            return super(PipelineDiGraph, cls).__new__(cls), False
        return PipelineDiGraph.G, True

    def __init__(self, action: Action = None):
        """Initialize the Pipeline DiGraph.
        Be sure to consider the case of a PR object whose output VR's are not used as inputs anywhere?"""
        # 1. Ensure it is a MultiDiGraph.
        super().__init__()
        self.build_pl_from_db(action = action)

    def add_node(self, node_for_adding: "ResearchObject", 
                 vr_name_in_code: str = None, 
                 vr: Variable = None, 
                 action: Action = None, 
                 is_input: bool = False,
                 value: Any = None,
                 **attr):
        """Add a node to the Pipeline DiGraph.
        By providing a vr_name_in_code, the Output VR is set for the node."""        
        super().add_node(node_for_adding, **attr)
        if not vr_name_in_code:            
            return

        # Add to the database.
        show = True
        target_pr_id = None
        source_pr_id = None
        if is_input:
            target_pr_id = node_for_adding.id
        else:
            source_pr_id = node_for_adding.id            
        is_lookup = int(False)
        pr_idx = 0           
        vr_id = vr.id if vr else None
        is_active = 1
        if not isinstance(vr, Variable):
            value = value # Hard-coded
        else:
            value = vr.hard_coded_value if vr and vr.hard_coded_value else None
        params = (action.id_num, target_pr_id, vr_name_in_code, source_pr_id, vr_id, value, pr_idx, is_lookup, int(show), is_active)
        action.add_sql_query(None, "pipeline_insert", params)

    def add_edge(self, source_object: "ResearchObject", target_object: "ResearchObject", vr: "Variable", tmp: bool = False, action: Action = None, is_input: bool = True):
        """Add an edge to the Pipeline DiGraph.
        source_object and target_object must both exist. The inputs & outputs must be fully formed."""
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        # Check if there's a VR.
        if not vr:
            raise ValueError("No VR provided.")
        # For inputs & outputs, not edges.
        if not target_object:
            self.add_node(source_object, action=action)
            return
        if not source_object:
            self.add_node(target_object, action=action)
            return
        # Check for duplicate edges.
        if source_object and target_object and self.has_edge(source_object, target_object, key = vr):
            return
        
        # Add the edge to the DiGraph.
        super().add_edge(source_object, target_object, key = vr)
        if not nx.is_directed_acyclic_graph(self):
            super().remove_edge(source_object, target_object, key = vr)
            raise ValueError("The edge would create a cycle!")
        
        if tmp:
            self.remove_edge(source_object, target_object, key = vr)
            return
        
        # Convert the edge to a database entry.
        show = False
        target_pr_id = target_object.id
        source_pr_id = source_object.id
        if not isinstance(source_object, Logsheet):
            source_vr_name_in_code = [key for key in source_object.outputs.keys() if source_object.outputs[key]["main"]["vr"] == vr.id][0]
        else:
            header_vrs = [header[3] for header in source_object.headers]
            header_names = [header[0] for header in source_object.headers]
            source_vr_name_in_code = header_names[header_vrs.index(vr)] if vr in header_vrs else None
        
        target_vr_name_in_code = [key for key in target_object.inputs.keys() if target_object.inputs[key]["main"]["vr"] == vr.id][0]
                
        vr_id = target_object.inputs[target_vr_name_in_code]["main"]["vr"]
        pr_idx = target_object.inputs[target_vr_name_in_code]["main"]["pr"].index(source_pr_id)
        is_lookup = int(False)
        params = (action.id_num, target_pr_id, target_vr_name_in_code, source_pr_id, vr_id, None, pr_idx, is_lookup, int(show), 1)
        action.add_sql_query(None, "pipeline_insert", params)

        # # Lookup edges.
        is_lookup = int(True)
        lookup_vr_id = target_object.inputs[target_vr_name_in_code]["lookup"]["vr"]
        for lookup_pr_id in target_object.inputs[target_vr_name_in_code]["lookup"]["pr"]:
            lookup_vr_name_in_code = [key for key in target_object.inputs.keys() if target_object.inputs[key]["lookup"]["vr"] == lookup_vr_id][0]
            lookup_vr_id = target_object.inputs[lookup_vr_name_in_code]["lookup"]["vr"]
            lookup_pr_idx = target_object.inputs[lookup_vr_name_in_code]["lookup"]["pr"].index(lookup_pr_id)
            params = (action.id_num, target_pr_id, target_vr_name_in_code, lookup_pr_id, lookup_vr_id, None, lookup_pr_idx, int(True), int(show), 1)
            action.add_sql_query(None, "pipeline_insert", params)
            

    def build_pl_from_db(self, action: Action = None):
        """Reads the Pipeline DiGraph from the database."""
        from ResearchOS.research_object import ResearchObject
        if PipelineDiGraph.G:
            return
        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name="Build_PL")

        # Get all nodes & edges
        sqlquery_raw = "SELECT target_pr_id, vr_name_in_code, source_pr_id, vr_id FROM pipeline WHERE is_active = 1"
        sqlquery = sql_order_result(action, sqlquery_raw, ["edge_id"], single = True, user = True, computer = False)
        result = action.conn.cursor().execute(sqlquery).fetchall()
        if not result:
            PipelineDiGraph.G = self
            return        

        # Add nodes & edges to the DiGraph.
        subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
        for row in result:
            # Row params to source & target & VR objects.
            target_pr = None
            source_pr = None
            target_pr_id = row[0]
            source_pr_id = row[2]
            vr_id = row[3]
            if target_pr_id:
                target_pr_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and target_pr_id.startswith(cls.prefix)][0]
                target_pr = target_pr_cls(id = target_pr_id, action=action)
            if source_pr_id:
                source_pr_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and source_pr_id.startswith(cls.prefix)][0]            
                source_pr = source_pr_cls(id = source_pr_id, action=action)
            vr = Variable(id = vr_id, action=action) if vr_id else None
            if source_pr and target_pr:            
                self.add_edge(source_pr, target_pr, vr = vr)     
            else:
                if source_pr:
                    self.add_node(source_pr, is_input=False)
                if target_pr:
                    self.add_node(target_pr, is_input=True)

        PipelineDiGraph.G = self

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
    # from src.research_objects import plots    
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
    """Write the inputs or outputs of a ResearchObject to the database.
    "puts" is the dict of inputs or outputs for that ResearchObject."""
    from ResearchOS.research_object import ResearchObject
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    return_conn = False
    if action is None:
        action = Action(name="Build_PL")
        return_conn = True

    G = PipelineDiGraph(action=action)
    params_list = puts_dict_to_params_list(puts, action, ro, is_input)
    subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)

    # Check if there are any edges that have been removed. These would be vr_name_in_code's for this target_pr_id that are not in the puts.
    vr_names = [vr_name for vr_name in prev_puts.keys() if vr_name not in puts.keys()] if puts else []
    for vr_name in vr_names:              
        params = (action.id_num, ro.id, vr_name, None, None, None, 0, int(False), int(False), int(False)) # Some default settings.
        action.add_sql_query(None, "pipeline_insert", params)

    # Check that there are no cycles being introduced.
    for param in params_list:        
        # Main & lookup.
        source_id = param[3]
        target_id = param[1]        
        source_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix.startswith(source_id[0:2])][0] if source_id else None
        target_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix.startswith(target_id[0:2])][0] if target_id else None
        source = source_cls(id = source_id) if source_cls else None
        target = target_cls(id = target_id) if target_cls else None
        vr = Variable(id=param[4], action=action) if param[4] else None
        if not param[4] and (hasattr(ro, "import_file_vr_name") and param[2] == ro.import_file_vr_name):
            vr = Variable(id=ro.inputs[param[2]]["main"]["vr"], action=action)
        if source_cls and target_cls:
            G.add_edge(source, target, vr=vr, action=action)            
        else:
            value = param[5]
            if source_cls:
                G.add_node(source, action=action, is_input = is_input, vr_name_in_code = param[2], vr = vr, value=value)
            if target_cls:
                G.add_node(target, action=action, is_input = is_input, vr_name_in_code = param[2], vr = vr, value=value)

    if return_conn:
        action.commit = True
        action.execute()


def puts_dict_to_params_list(puts: dict, action: Action = None, ro: "ResearchObject" = None, is_input: bool = True) -> list:
    """Convert the JSON-serializable dict of inputs or outputs to a list of params tuples for the database."""
    params_list = []    
    for key, put in puts.items():   
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
        if not pr_ids:
            pr_ids = [None]
        lookup_vr_id = put["lookup"]["vr"]
        lookup_vr_id = lookup_vr_id.id if isinstance(lookup_vr_id, Variable) else lookup_vr_id
        lookup_pr_ids = put["lookup"]["pr"] if lookup_vr_id else []
        if not lookup_pr_ids:
            lookup_pr_ids = [None] if lookup_vr_id else []
        # Hard-coded value.
        if value:
            params = (action.id_num, ro.id, key, None, None, json.dumps(value), 0, int(False), int(show), int(True))
            if not params in params_list:
                params_list.append(params)
            continue
        elif vr_id:
            # Import file VR name.
            if hasattr(ro, "import_file_vr_name") and key == ro.import_file_vr_name:                
                params = (action.id_num, ro.id, key, None, None, None, 0, int(False), int(show), int(True))
                if not params in params_list:
                    params_list.append(params)    
                continue

        for order_num, pr in enumerate(pr_ids):
            if is_input:
                value = value if not value else json.dumps(value)
                params = (action.id_num, ro.id, key, pr, vr_id, value, order_num, int(False), int(show), int(True))
            else: # Swap source & target for the output.
                params = (action.id_num, pr, key, ro.id, vr_id, None, order_num, int(False), int(show), int(True))
            if not params in params_list:
                params_list.append(params)
        for order_num, pr in enumerate(lookup_pr_ids):
            params = (action.id_num, ro.id, key, pr, lookup_vr_id, None, order_num, int(True), int(show), int(True))
            if not params in params_list:
                params_list.append(params)

    return params_list