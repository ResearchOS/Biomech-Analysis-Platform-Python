from src.ResearchOS import ResearchObject
from typing import Any

class PipelineObject(ResearchObject):
    """Parent class of all pipeline objects: Projects, Analyses, Logsheets, Process Groups, Processes, Variables, SpecifyTrials, Views"""

    # def __setattr__(self, __name: str, __value: Any) -> None:
    #     super().__setattr__(__name, __value)        
        
    #     # Set attributes of the object in the database.
    #     pass