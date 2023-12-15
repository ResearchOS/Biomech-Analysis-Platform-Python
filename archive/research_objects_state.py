# from research_object import ResearchObject
# from typing import Union

class ROState():
    """The set of states for the research object(s) involved in a particular action.
    Each Research Object State (ROState) consists of all of the modified attributes of all of the objects."""
    def __init__(self, objs: list[ResearchObject] = []):
        self.research_objs = {}
        for obj in objs:
            id = obj.id
            self.research_objs[id] = obj

    def set_state(self):
        """Set the state of each research object."""
        for research_obj in self.research_objs.keys():
            research_obj.set_attr()