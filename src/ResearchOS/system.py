from .research_object import ResearchObject

class System(ResearchObject):
    """The internal representation of the entire ResearchOS system.
    Used to manage settings like the current user object ID, etc. that can't be encapsulated by any other object because they're system-wide."""