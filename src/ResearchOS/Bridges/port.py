# from research_object import ResearchObject

class Port():
    """Base class for the various port mechanisms to connect between DiGraphs."""

    def __hash__(self) -> int:
        return self.vr_name_in_code

    def __init__(self):
        """Initializes the Port object. Does NOT use Research Object's constructor."""
        pass