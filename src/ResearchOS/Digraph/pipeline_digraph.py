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

    def add_node(self, node_for_adding: "ResearchObject", action: Action = None, **attr):
        """Add a node to the Pipeline DiGraph."""
        if self.has_node(node_for_adding):
            return
        super().add_node(node_for_adding, **attr)

        # Add to the database.
        show = True
        parent_ro_id = node_for_adding.id
        is_lookup = int(False)
        pr_idx = 0   
        vr_name_in_code = None
        vr_id = None
        source_pr_id = None
        is_active = 1
        params = (action.id_num, parent_ro_id, vr_name_in_code, source_pr_id, vr_id, None, pr_idx, is_lookup, int(show), is_active)
        action.add_sql_query(None, "pipeline_insert", params)


    def add_edge(self, source_object: "ResearchObject", target_object: "ResearchObject", vr: "Variable", tmp: bool = False, action: Action = None,):
        """Add an edge to the Pipeline DiGraph.
        1. If source_object = None, this is an input NOT an edge (i.e. no edge has been previously found).
        2. If target_object = None, this is an output NOT an edge.
        3. If source_object != target_object, this is an edge!"""
        from ResearchOS.PipelineObjects.logsheet import Logsheet
        if not vr:
            raise ValueError("No VR provided.")
        if source_object and target_object and self.has_edge(source_object, target_object, key = vr):
            return
        if source_object and target_object:
            super().add_edge(source_object, target_object, vr = vr)
        else:
            obj = source_object if source_object else target_object
            self.add_node(obj, action=action)
            return
        if nx.is_directed_acyclic_graph(self):
            super().remove_edge(source_object, target_object, vr = vr) if source_object is not None else super().remove_node(target_object)
            raise ValueError("The edge would create a cycle!")
        
        if tmp:
            super().remove_edge(source_object, target_object, vr = vr)
            return
        
        # Add the edge to the database.
        show = False
        parent_ro_id = target_object.id
        source_pr_id = source_object.id if source_object else None
        if not isinstance(source_object, Logsheet):
            src_vr_name_in_code = [key for key in source_object.outputs.keys() if source_object.outputs[key]["main"]["vr"] == vr.id][0] if source_object else None
        else:
            header_vrs = [header[3] for header in source_object.headers]
            header_names = [header[0] for header in source_object.headers]
            src_vr_name_in_code = header_names[header_vrs.index(vr)] if vr in header_vrs else None
        target_vr_name_in_code = [key for key in target_object.inputs.keys() if target_object.inputs[key]["main"]["vr"] == vr.id][0]
        vr_id = target_object.inputs[target_vr_name_in_code]["main"]["vr"]        
        pr_idx = source_object.outputs[src_vr_name_in_code]["main"]["pr"].index(source_pr_id)
        is_lookup = int(False)
        params = (action.id_num, parent_ro_id, target_vr_name_in_code, source_pr_id, vr_id, None, pr_idx, is_lookup, int(show), 1)
        action.add_sql_query(None, "pipeline_insert", params)

        # Lookup edges.
        is_lookup = int(True)
        lookup_vr_id = source_object.inputs[src_vr_name_in_code]["lookup"]["vr"] if source_object else None
        for lookup_pr_id in source_object.inputs[src_vr_name_in_code]["lookup"]["pr"]:
            
            lookup_vr_name_in_code = [key for key in source_object.inputs.keys() if source_object.inputs[key]["lookup"]["vr"] == lookup_vr_id][0]
            lookup_vr_id = source_object.inputs[lookup_vr_name_in_code]["lookup"]["vr"]
            lookup_pr_idx = source_object.inputs[lookup_vr_name_in_code]["lookup"]["pr"].index(lookup_pr_id)
            params = (action.id_num, parent_ro_id, target_vr_name_in_code, lookup_pr_id, lookup_vr_id, None, lookup_pr_idx, int(True), int(show), 1)
            action.add_sql_query(None, "pipeline_insert", params)
            

    def build_pl_from_db(self, action: Action = None):
        """Build the Pipeline DiGraph from the database."""
        from ResearchOS.research_object import ResearchObject
        if PipelineDiGraph.G:
            return
        return_conn = True
        if action is None:
            return_conn = False
            action = Action(name="Build_PL")

        sqlquery_raw = "SELECT parent_ro_id, vr_name_in_code, source_pr_id, vr_id FROM pipeline WHERE is_active = 1 AND source_pr_id IS NOT pipeline.parent_ro_id"
        sqlquery = sql_order_result(action, sqlquery_raw, ["edge_id"], single = True, user = True, computer = False)
        result = action.conn.cursor().execute(sqlquery).fetchall()
        if not result:
            PipelineDiGraph.G = self
            return

        subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
        for row in result:
            parent_ro_id = row[0]
            vr_name_in_code = row[1]
            source_pr_id = row[2]
            vr_id = row[3]
            parent_ro_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and parent_ro_id.startswith(cls.prefix)][0]
            source_pr_cls = [cls for cls in subclasses if hasattr(cls, "prefix") and source_pr_id.startswith(cls.prefix)][0]
            parent_ro = parent_ro_cls(id = parent_ro_id)
            source_pr = source_pr_cls(id = source_pr_id) if source_pr_id is not None else None # Nodes without edges.
            vr = Variable(id = vr_id)
            super().add_edge(source_pr, parent_ro, vr_name_in_code = vr_name_in_code, vr = vr)     

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
    return G