from abc import abstractmethod

from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject

class Project(PipelineObject):
    """A project is a collection of analyses.
    Class-specific Attributes:
    1. current_analysis_id: The ID of the current analysis for this project.
    2. current_dataset_id: The ID of the current dataset for this project.
    3. project path: The root folder location of the project."""

    prefix: str = "PJ"

    @abstractmethod
    def new_current(name: str) -> "Project":
        """Create a new analysis and set it as the current analysis for the current project."""
        from src.ResearchOS.PipelineObjects.analysis import Analysis
        pj = Project(name = name)
        an = Analysis(name = pj.name + "_Default_Analysis")
        pj.current_analysis_id = an.id
        return pj  
    
    #################### Start class-specific attributes ###################

    def get_project_path(self) -> str:
        """Return the project path."""        
        return self.project_path
    
    def set_project_path(self, path: str) -> None:
        """Set the project path."""        
        self.project_path = path    

    def get_current_analysis_id(self) -> str:
        """Return the current analysis object ID for this project."""
        # from src.ResearchOS.PipelineObjects.analysis import Analysis
        return self.current_analysis_id
    
    def set_current_analysis_id(self, analysis_id: str) -> None:
        """Set the current analysis object ID for this project."""
        from src.ResearchOS.PipelineObjects.analysis import Analysis
        self.validate_id_class(analysis_id, Analysis)
        self.current_analysis_id = analysis_id

    def get_current_dataset_id(self) -> str:
        """Return the current dataset object ID for this project."""
        # from src.ResearchOS.DataObjects.dataset import Dataset
        return self.current_dataset_id
    
    def set_current_dataset_id(self, dataset_id: str) -> None:
        """Set the current dataset object ID for this project."""
        from src.ResearchOS.DataObjects.dataset import Dataset
        self.validate_id_class(dataset_id, Dataset)
        self.current_dataset_id = dataset_id

    #################### Start Source objects ####################

    def get_users(self) -> list:
        """Return a list of user objects that belong to this project. Identical to Dataset.get_users()"""
        from src.ResearchOS.user import User
        us_ids = self._get_all_source_object_ids(cls = User)
        return [User(id = us_id) for us_id in us_ids]
    
    #################### Start Target objects ####################
            
    def get_analyses(self) -> list["Analysis"]:        
        """Return a list of analysis objects in the project."""
        from src.ResearchOS.PipelineObjects.analysis import Analysis
        an_ids = self._get_all_target_object_ids(cls = Analysis)
        return [Analysis(id = an_id) for an_id in an_ids]
    
    def add_analysis_id(self, analysis_id: str):
        """Add an analysis to the project."""
        from src.ResearchOS.PipelineObjects.analysis import Analysis        
        self._add_target_object_id(analysis_id, cls = Analysis)

    def remove_analysis_id(self, analysis_id: str):
        """Remove an analysis from the project."""
        from src.ResearchOS.PipelineObjects.analysis import Analysis        
        self._remove_target_object_id(analysis_id, cls = Analysis)

if __name__=="__main__":
    from src.ResearchOS.PipelineObjects.analysis import Analysis
    pj = Project(name = "Test")    
    an = Analysis(name = "Test_Analysis")
    pj.current_analysis_id = an.id
    print(pj)