from typing import Any

from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.db_connection_factory import DBConnectionFactory

all_default_attrs = {}
all_default_attrs["current_analysis_id"] = None
all_default_attrs["current_dataset_id"] = None
all_default_attrs["project_path"] = None

complex_attrs_list = []

class Project(PipelineObject):
    """A project is a collection of analyses.
    Class-specific Attributes:
    1. current_analysis_id: The ID of the current analysis for this project.
    2. current_dataset_id: The ID of the current dataset for this project.
    3. project path: The root folder location of the project."""

    prefix: str = "PJ"

    # def __init__(self, **kwargs):
    #     """Initialize the attributes that are required by ResearchOS.
    #     Other attributes can be added & modified later."""
    #     super().__init__(all_default_attrs, **kwargs)

    # def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
    #     """Set the attribute value. If the attribute value is not valid, an error is thrown."""
    #     ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)

    def load(self) -> None:
        """Load the dataset-specific attributes from the database in an attribute-specific way."""
        PipelineObject.load(self) # Load the attributes specific to it being a PipelineObject.
    
    def validate_current_analysis_id(self, id: str) -> None:
        """Validate the current analysis ID. If it is not valid, the value is rejected."""        
        if not isinstance(id, str):
            raise ValueError("Specified value is not a string!")
        if not self.is_id(id):
            raise ValueError("Specified value is not an ID!")
        parsed_id = self.parse_id(id)
        if parsed_id[0] != "AN":
            raise ValueError("Specified ID is not an Analysis!")
        if not self.object_exists(id):
            raise ValueError("Analysis does not exist!")
    
    def validate_current_dataset_id(self, id: str) -> None:
        """Validate the current dataset ID. If it is not valid, the value is rejected."""
        if not self.is_id(id):
            raise ValueError("Specified value is not an ID!")
        parsed_id = self.parse_id(id)
        if parsed_id[0] != "DS":
            raise ValueError("Specified ID is not a Dataset!")
        if not self.object_exists(id):
            raise ValueError("Dataset does not exist!")
        
    def validate_project_path(self, path: str) -> None:
        """Validate the project path. If it is not valid, the value is rejected."""
        # 1. Check that the path exists in the file system.
        import os
        if not isinstance(path, str):
            raise ValueError("Specified path is not a string!")
        if not os.path.exists(path):
            raise ValueError("Specified path is not a path or does not currently exist!")        

    #################### Start Source objects ####################
    def get_users(self) -> list:
        """Return a list of user objects that belong to this project. Identical to Dataset.get_users()"""
        from ResearchOS.user import User
        us_ids = self._get_all_source_object_ids(cls = User)
        return self._gen_obj_or_none(us_ids, User)
    
    #################### Start Target objects ####################
    def get_analyses(self) -> list["Analysis"]:        
        """Return a list of analysis objects in the project."""
        from ResearchOS.PipelineObjects.analysis import Analysis
        an_ids = self._get_all_target_object_ids(cls = Analysis)
        return self._gen_obj_or_none(an_ids, Analysis)
    
    def add_analysis_id(self, analysis_id: str):
        """Add an analysis to the project."""
        from ResearchOS.PipelineObjects.analysis import Analysis        
        self._add_target_object_id(analysis_id, cls = Analysis)

    def remove_analysis_id(self, analysis_id: str):
        """Remove an analysis from the project."""
        from ResearchOS.PipelineObjects.analysis import Analysis        
        self._remove_target_object_id(analysis_id, cls = Analysis)

    #################### Start class-specific methods ####################
    def open_project_path(self) -> None:
        """Open the project's path in the Finder/File Explorer."""
        path = self.project_path
        self._open_path(path)
    

if __name__=="__main__":
    from ResearchOS import Analysis
    pj = Project(name = "Test")
    an1 = Analysis()      
    an2 = Analysis()
    pj.current_analysis_id = an1.id
    pj.add_analysis_id(an2.id)
    ans = pj.get_analyses()
    pj.project_path = 'Test'
    print(pj)