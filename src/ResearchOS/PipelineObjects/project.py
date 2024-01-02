from abc import abstractmethod

from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject

class Project(PipelineObject):
    """A project is a collection of analyses.
    Class-specific Attributes:
    1. current_analysis_id: The ID of the current analysis for this project.
    2. current_dataset_id: The ID of the current dataset for this project.
    3. project path: The root folder location of the project."""

    prefix: str = "PJ"      
    DEFAULT_CURRENT_ANALYSIS_ID: str = ""
    DEFAULT_CURRENT_DATASET_ID: str = ""
    DEFAULT_PROJECT_PATH: str = ""  

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Project)
    
    @abstractmethod
    def new_current(name: str) -> "Project":
        """Create a new analysis and set it as the current analysis for the current project.
        Returns the project & analysis objects as a tuple."""
        from src.ResearchOS.PipelineObjects.analysis import Analysis
        pj = Project(name = name)
        an = Analysis(name = pj.name + "_Default_Analysis")
        pj.current_analysis_id = an.id
        return pj, an
    
    #################### Start class-specific attributes ###################
    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        attrs = {}
        attrs["current_analysis_id"] = Project.DEFAULT_CURRENT_ANALYSIS_ID # The current analysis for the project.
        attrs["current_dataset_id"] = Project.DEFAULT_CURRENT_DATASET_ID # The current dataset for the project.
        attrs["project_path"] = Project.DEFAULT_PROJECT_PATH # The root folder for the current project.
        super().__init__(attrs = attrs)            
    
    def validate_current_analysis_id(self, id: str):
        """Validate the current analysis ID. If it is not valid, the value is rejected."""        
        if not self.is_id(id):
            raise ValueError("Specified value is not an ID!")
        parsed_id = self.parse_id(id)
        if parsed_id[0] != "AN":
            raise ValueError("Specified ID is not an Analysis!")
        if not self.object_exists(id):
            raise ValueError("Analysis does not exist!")
    
    def validate_current_dataset_id(self, id: str):
        """Validate the current dataset ID. If it is not valid, the value is rejected."""
        if not self.is_id(id):
            raise ValueError("Specified value is not an ID!")
        parsed_id = self.parse_id(id)
        if parsed_id[0] != "DS":
            raise ValueError("Specified ID is not a Dataset!")
        if not self.object_exists(id):
            raise ValueError("Dataset does not exist!")
        
    def validate_project_path(self, path: str):
        """Validate the project path. If it is not valid, the value is rejected."""
        # 1. Check that the path exists in the file system.
        import os
        if not os.path.exists(path):
            raise ValueError("Specified path does not exist!")        

    def json_translate_XXX(self) -> type:
        """Convert the attribute from JSON to the proper data type/format, if json.loads is not sufficient.
        XXX is the exact name of the attribute. Method name must follow this format."""

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
        return self._gen_obj_or_none(an_ids, Analysis)
    
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
    # an = Analysis(name = "Test_Analysis")
    pj.current_analysis_id = an.id
    print(pj)