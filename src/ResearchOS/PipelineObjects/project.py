from abc import abstractmethod
from typing import Any, Type

from ResearchOS import ResearchObject
from ResearchOS.PipelineObjects import PipelineObject
from ResearchOS.PipelineObjects import Analysis
from ResearchOS.DataObjects import Dataset
from ResearchOS import Action

class Project(PipelineObject):

    prefix = "PJ"       

    def check_valid_attrs(self):
        if not self.is_id(self.current_analysis_id):
            raise ValueError        

    @abstractmethod
    def new(name: str) -> "Project":
        action = Action(name = "New Project" + name)
        pj = Project(name = name)
        action.close()
        return pj

    @abstractmethod
    def new_current(name: str) -> "Project":
        action = Action(name = "New Current Project" + name)
        pj = Project.new(name = name)
        an = Analysis.new_current(name = pj.name + "_Default")
        pj.set_current_analysis(an.id)
        action.close()
        return pj

    def set_current_analysis(self, id: str) -> None:
        """Set an analysis as the current one for the project."""
        self.current_analysis_id = id

    @abstractmethod
    def load(id: str, action_id: str = None) -> ResearchObject:        
        return ResearchObject.load(id = id, cls = Project, action_id = action_id)
    
    def analyses(self):
        """Return a list of analyses in the project."""
        return self.children(cls = Analysis)
    
    def current_analysis(self) -> Analysis:
        """Return the current analysis in the project."""
        return Analysis.load(id = self.current_analysis_id)
    
    def current_dataset(self):
        """Return the current dataset in the project."""        
        return Dataset.load(id = self.current_dataset_id)
    
    def datasets(self):
        """Return a list of datasets in the project."""        
        return self.children(cls = Dataset)

if __name__=="__main__":
    pj = Project.new_current(name = "Test")
    print(pj)