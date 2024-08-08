

from ResearchOS.action import Action
from ResearchOS.Bridges.put import Put
from ResearchOS.Bridges.let import Let
from ResearchOS.Bridges.pipeline_parts import PipelineParts

class LetPut(PipelineParts):

    cls_name = "LetPut"
    table_name = "lets_puts"
    id_col = "let_put_id"
    col_names = ["put_id", "let_id"]
    insert_query_name = "lets_puts_insert"
    init_attr_names = ["put", "let"]

    def __init__(self, id: int = None, 
                 put: Put = None, 
                 let: Let = None, 
                 put_id: str = None,
                 let_id: str = None,
                 action: Action = None):
        """Initializes the LetPut object."""
        if put is not None:
            put_id = put.id
        if let is not None:
            let_id = let.id
        self.put = put
        self.let = let
        self.put_id = put_id
        self.let_id = let_id
        super().__init__(id = id, action = action)

    def load_from_db(self, let_id: str, put_id: str, action: Action):
        """Load the let_put objects from the database."""
        put = Put(id = put_id, action = action) if self.put is None else self.put
        let = Let(id = let_id, action = action) if self.let is None else self.let
        self.put = put
        self.let = let
        
