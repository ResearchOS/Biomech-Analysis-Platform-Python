from research_object import ResearchObject
from typing import Any

class PipelineObject(ResearchObject):
    """Parent class of all pipeline objects: Projects, Analyses, Logsheets, Process Groups, Processes, Variables, SpecifyTrials, Views"""

    table_name = "PipelineObjects"

    def __setattr__(self, __name: str, __value: Any) -> None:
        super().__setattr__(__name, __value)        
        if __name == "id":
            return
        
        # Create the object in the database.
        pass