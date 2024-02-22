from typing import TYPE_CHECKING
from networkx import MultiDiGraph

from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

class ROGraph(MultiDiGraph):
    """This class represents a mutli-digraph of the relations of the pipeline objects to one another."""
    
    def __init__(self, obj_ids: list) -> None:
        """Create a MultiDiGraph with all of the ID's as nodes and the edges as the relations between the objects."""
        pass

    def __getitem__(self, n) -> "ResearchObject":
        """Given a research object ID, return the research object whether previously loaded or not."""
        value = super().__getitem__(n)
        if value not in ResearchObjectHandler.instances:
            cls = ResearchObjectHandler._prefix_to_class(IDCreator().get_prefix(n))
            value = cls(id = n)
        return value