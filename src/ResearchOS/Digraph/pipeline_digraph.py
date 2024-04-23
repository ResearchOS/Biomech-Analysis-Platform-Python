from typing import TYPE_CHECKING

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

    def add_node(self, node_for_adding: "ResearchObject", action: Action = None, is_input: bool = False, **attr):
        """Add a node to the Pipeline DiGraph"""
        if self.has_node(node_for_adding):
            return
        super().add_node(node_for_adding, **attr)

        # Add to the database.
        show = True
        source_pr_id = None
        target_pr_id = node_for_adding.id            
        is_lookup = int(False)
        pr_idx = 0   
        vr_name_in_code = None
        vr_id = None
        is_active = 1
        params = (action.id_num, target_pr_id, vr_name_in_code, source_pr_id, vr_id, None, pr_idx, is_lookup, int(show), is_active)
        action.add_sql_query(None, "pipeline_insert", params)

    def add_edge(self, source_object: "ResearchObject", target_object: "ResearchObject", vr: "Variable", tmp: bool = False, action: Action = None, is_input: bool = True):
        """Add an edge to the Pipeline DiGraph.
        1. If source_object = None, this is an input NOT an edge (i.e. no edge has been previously found).
        2. If target_object = None, this is an output NOT an edge.
        3. If source_object != target_object, this is an edge!"""
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        if not vr:
            raise ValueError("No VR provided.")
        if isinstance(source_object, Logsheet):
            self.add_node(source_object, action=action)
        if source_object and target_object and self.has_edge(source_object, target_object, key = vr):
            return
        is_disconnected = False
        if source_object and target_object:
            super().add_edge(source_object, target_object, vr = vr)
        else:
            is_disconnected = True
            # obj = source_object if source_object else target_object
            # self.add_node(obj, action=action)
            # return
        if not nx.is_directed_acyclic_graph(self):
            super().remove_edge(source_object, target_object, vr = vr) if source_object is not None else super().remove_node(target_object)
            raise ValueError("The edge would create a cycle!")
        
        if tmp:
            super().remove_edge(source_object, target_object, vr = vr)
            return
        
        # Add the edge to the database.
        show = False
        target_pr_id = target_object.id
        source_pr_id = source_object.id if not is_disconnected else None
        if not isinstance(source_object, Logsheet):
            src_vr_name_in_code = [key for key in source_object.outputs.keys() if source_object.outputs[key]["main"]["vr"] == vr.id][0] if source_object else None
        else:
            header_vrs = [header[3] for header in source_object.headers]
            header_names = [header[0] for header in source_object.headers]
            src_vr_name_in_code = header_names[header_vrs.index(vr)] if vr in header_vrs else None
        
        target_vr_name_in_code = [key for key in target_object.inputs.keys() if target_object.inputs[key]["main"]["vr"] == vr.id][0] if not is_disconnected else None
        vr_name_in_code = target_vr_name_in_code
        if is_disconnected and is_input:
            vr_name_in_code = src_vr_name_in_code
                
        vr_id = source_object.outputs[target_vr_name_in_code]["main"]["vr"]
        pr_idx = target_object.inputs[src_vr_name_in_code]["main"]["pr"].index(source_pr_id)
        is_lookup = int(False)
        params = (action.id_num, target_pr_id, vr_name_in_code, source_pr_id, vr_id, None, pr_idx, is_lookup, int(show), 1)
        action.add_sql_query(None, "pipeline_insert", params)

        # Lookup edges.
        is_lookup = int(True)
        lookup_vr_id = source_object.inputs[src_vr_name_in_code]["lookup"]["vr"] if source_object else None
        for lookup_pr_id in source_object.inputs[src_vr_name_in_code]["lookup"]["pr"]:
            
            lookup_vr_name_in_code = [key for key in source_object.inputs.keys() if source_object.inputs[key]["lookup"]["vr"] == lookup_vr_id][0]
            lookup_vr_id = source_object.inputs[lookup_vr_name_in_code]["lookup"]["vr"]
            lookup_pr_idx = source_object.inputs[lookup_vr_name_in_code]["lookup"]["pr"].index(lookup_pr_id)
            params = (action.id_num, target_pr_id, target_vr_name_in_code, lookup_pr_id, lookup_vr_id, None, lookup_pr_idx, int(True), int(show), 1)
            action.add_sql_query(None, "pipeline_insert", params)
            

    def build_pl_from_db(self, action: Action = None):
        """Reads the Pipeline DiGraph from the database."""
        from ResearchOS.research_object import ResearchObject
        if PipelineDiGraph.G:
            return
        return_conn = True
        if action is None:
            return_conn = False
            action = Action(name="Build_PL")

        sqlquery_raw = "SELECT parent_ro_id, vr_name_in_code, source_pr_id, vr_id FROM pipeline FROM pipeline WHERE is_active = 1"
        sqlquery = sql_order_result(action, sqlquery_raw, ["edge_id"], single = True, user = True, computer = False)
        result = action.conn.cursor().execute(sqlquery).fetchall()
        if not result:
            PipelineDiGraph.G = self
            return        

        subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
        for row in result:
            # Row params to input dict
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
            vr = Variable(id = vr_id, action=action)
            if source_pr and target_pr:            
                super().add_edge(source_pr, target_pr, vr = vr)     
            else:
                if source_pr:
                    super().add_node(source_pr)
                if target_pr:
                    super().add_node(target_pr)

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

def make_all_edges(ro: "ResearchObject", action: Action = None, puts: dict = None, is_input: bool = True):
    """Make all the edges for a Research Object, given the puts from another Research Object. (?)
    "ro" is the Research Object.
    "puts" is the dict of inputs or outputs for that ResearchObject.
    If the pr.id matches the ro.id, then this is not an OUTPUT. This is included so that I can find source_pr's."""
    from ResearchOS.research_object import ResearchObject
    from ResearchOS.PipelineObjects.process import Process
    from ResearchOS.PipelineObjects.logsheet import Logsheet
    return_conn = False
    if action is None:
        action = Action(name="Build_PL")
        return_conn = True

    if is_input:
        key = "inputs"
    else:
        key = "outputs"

    G = PipelineDiGraph()
    params_list = []
    subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
    for key, put in puts.items():
        show = put["show"]
        main_vr = put["main"]["vr"]
        value = main_vr if not (isinstance(main_vr, Variable) or isinstance(main_vr, str) and main_vr.startswith("VR")) else None
        vr_id = None
        if not value:
            vr_id = main_vr.id if isinstance(main_vr, Variable) else main_vr
        pr_ids = put["main"]["pr"] if vr_id else []
        if pr_ids and not isinstance(pr_ids, list):
            pr_ids = [pr_ids]
        # if all([pr_id == ro.id for pr_id in pr_ids]):
        #     return # This is an output, and therefore not an edge.
        lookup_vr_id = put["lookup"]["vr"]
        lookup_vr_id = lookup_vr_id.id if isinstance(lookup_vr_id, Variable) else lookup_vr_id
        lookup_pr_ids = put["lookup"]["pr"] if lookup_vr_id else []
        # Hard-coded value.
        if value:
            G.add_hard_coded_put(ro, key, value, action)            
            # params = (action.id_num, ro.id, key, None, None, json.dumps(value), 0, int(False), int(show), int(True))
            # if not action.is_redundant_params(None, "pipeline_insert", params):
            #     params_list.append(params)
        elif vr_id:
            # Import file VR name.
            if hasattr(ro, "import_file_vr_name") and key == ro.import_file_vr_name:
                G.add_import_file_vr_put(ro, key, vr_id, action)
                # params = (action.id_num, ro.id, key, None, None, None, 0, int(False), int(show), int(True))
                # if not action.is_redundant_params(None, "pipeline_insert", params):
                #     params_list.append(params)                
        for order_num, pr in enumerate(pr_ids):
            if pr.startswith(Process.prefix):
                pr = Process(id = pr, action=action)
            else:
                pr = Logsheet(id = pr, action=action)
            G.add_edge(pr, ro, vr=Variable(id=vr_id, action=action), tmp=False, action=action)
            # params = (action.id_num, ro.id, key, pr, vr_id, value, order_num, int(False), int(show), int(True))
            # if not action.is_redundant_params(None, "pipeline_insert", params):
            #     params_list.append(params)
        for order_num, pr in enumerate(lookup_pr_ids):
            if pr.startswith(Process.prefix):
                pr = Process(id = pr, action=action)
            else:
                pr = Logsheet(id = pr, action=action)
            G.add_edge(pr, ro, vr=Variable(id=lookup_vr_id, action=action), tmp=False, action=action)
            # params = (action.id_num, ro.id, key, pr, lookup_vr_id, None, order_num, int(True), int(show), int(True))
            # if not action.is_redundant_params(None, "pipeline_insert", params):
            #     params_list.append(params)

    # Check if there are any edges that have been removed. These would be vr_name_in_code's for this target_pr_id that are not in the puts.
    sqlquery_raw = "SELECT vr_name_in_code FROM pipeline WHERE target_pr_id = ? AND is_active = 1 AND"
    if is_input:
        operator = "IS NOT"
    else:
        operator = "IS"
    sqlquery_raw += " source_pr_id " + operator + " pipeline.target_pr_id" # Need to include the "pipeline." prefix because of the way sql_order_result works.
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