from ResearchOS import PipelineObject
from src.ResearchOS.action import Action

import os

class Logsheet(PipelineObject):

    prefix = "LG"

    def check_valid_attrs(self):
        # Need to check for the actual file existing, not just valid path format.
        if not os.path.exists(self.logsheet_path):
            raise ValueError
    
    def new(name: str) -> "Logsheet":
        action = Action(name = "New Logsheet" + name)
        lg = Logsheet(name = name)
        action.close()
        return lg