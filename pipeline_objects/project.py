from abc import abstractmethod
from typing import Any, Type

from research_object import ResearchObject
from pipeline_objects.pipeline_object import PipelineObject
from pipeline_objects.analysis import Analysis
from action import Action

class Project(PipelineObject):

    prefix = "PJ"    

    @abstractmethod
    def new_current(name: str):
        action = Action.new(name = "Init")
        pj = Project.new(name = name)
        an = Analysis.new_current(name = pj.name + "_Default")
        pj.set_current_analysis(an.id)
        action.close()

    def set_current_analysis(self, id: str):
        """Set an analysis as the current one for the project."""
        self.current_analysis_id = id

    @abstractmethod
    def load(id: str, action_id: str = None) -> ResearchObject:        
        return ResearchObject.load(id = id, cls = Project, action_id = action_id)

if __name__=="__main__":
    pj = Project.new_current(name = "Test")
    print(pj)