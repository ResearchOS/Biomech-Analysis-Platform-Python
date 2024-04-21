

from ResearchOS.action import Action
from ResearchOS.Bridges.put import Put
from ResearchOS.Bridges.let import Let
from ResearchOS.Bridges.pipeline_parts import PipelineParts

class LetPut(PipelineParts):

    cls_name = "LetPut"
    table_name = "lets_puts"
    id_col = "let_put_id"
    col_names = ["put_id", "let_id"]
    insert_query_name = "let_put_insert"

    def __init__(self, id: int = None, put: Put = None, let: Let = None, action: Action = None):
        self.put = put
        self.let = let
        super().__init__(id = id, action = action)

    def load_from_db(self, let_id: str, put_id: str):
        """Load the let_put objects from the database."""
        put = Put(id = put_id, action = self.action)
        let = Let(id = let_id, action = self.action)
        self.put = put
        self.let = let
        
