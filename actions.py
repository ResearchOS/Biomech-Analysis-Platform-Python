"""Comprised of one set of GUI widget states, and one set of research object states."""

from widgets_state import WState
from research_objects_state import ROState

class Action():
    def __init__(self, widgets_state: list[WState] = None, robjs_state: list[ROState] = None):        
        self.widgets_state = widgets_state
        self.research_objs_state = robjs_state
        self.action_previous = None
        self.action_next = None

    def __post_init__():
        pass