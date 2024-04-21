from typing import TYPE_CHECKING, Union, Any

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler

class Let():

    cls_name = "Let"
    table_name = "inlets_outlets"
    id_col = "let_id"
    col_names = ["is_input", "parent_ro_id", "vr_name_in_code", "show"]
    insert_query_name = "inlets_outlets_insert"

    def __init__(self, id: int = None,
                 is_input: bool = False,
                 parent_ro: "ResearchObject" = None,
                 vr_name_in_code: str = None,
                 show: bool = False,
                 action: Action = None):
        self.is_input = is_input
        self.parent_ro = parent_ro
        self.vr_name_in_code = vr_name_in_code
        self.show = show
        super().__init__(id = id, action = action)

    def load_from_db(self, is_input: bool, parent_ro_id: str, vr_name_in_code: str, show: bool):
        """Load the let objects from the database."""
        subclasses = ResearchObjectHandler._get_subclasses()
        cls = [cls for cls in subclasses if cls.prefix == parent_ro_id[0:2]][0]
        parent_ro = cls(id = parent_ro_id, action = self.action)
        self.is_input = is_input
        self.parent_ro = parent_ro
        self.vr_name_in_code = vr_name_in_code
        self.show = show
