from ResearchOS.action import Action
from ResearchOS.Bridges.pipeline_parts import PipelineParts
from ResearchOS.Bridges.letput import LetPut

class Edge(PipelineParts):

    cls_name = "Edge"
    table_name = "pipelineobjects_graph"
    id_col = "edge_id"
    col_names = ["source_let_put_id", "target_let_put_id"]
    insert_query_name = "pipelineobjects_graph_insert"
    init_attr_names = ["source_let_put_id", "target_let_put_id"]

    def __init__(self, id: int = None,
                    source_let_put_id: str = None,
                    target_let_put_id: str = None,
                    action: Action = None):
        self.source_let_put_id = source_let_put_id
        self.target_let_put_id = target_let_put_id
        super().__init__(id = id, action = action)    
        

    def __str__(self):
        subset_id = self.input_dynamic.parent_ro.subset.id if self.input_dynamic.parent_ro.subset is not None else None
        return f"""{self.output_dynamic.parent_ro.id}: {self.output_dynamic.vr_name_in_code} -> {self.input_dynamic.parent_ro.id}: {self.input_dynamic.vr_name_in_code} Subset: {subset_id}"""
    

    def load_from_db(self, source_let_put_id: str, target_let_put_id: str, action: Action = None):
        """Load the let_put objects from the database."""
        source_let_put = LetPut(id = source_let_put_id, action = action) if (hasattr(self, "source_let_put") and self.source_let_put is not None) else None
        target_let_put = LetPut(id = target_let_put_id, action = action) if (hasattr(self, "target_let_put") and self.target_let_put is not None) else None
        self.source_let_put = source_let_put
        self.target_let_put = target_let_put