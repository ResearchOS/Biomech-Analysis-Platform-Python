"""The set of states for the research object(s) involved in a particular action.
Each Research Object State (ROState) consists of all of the modified attributes of all of the objects."""

from research_objects import ResearchObject

class ROState():
    def __init__(self, objs: list[ResearchObject]):
        self.research_objs = {}
        for obj in objs:
            id = objs['id']
            del obj['id']
            self.research_objs[id] = obj