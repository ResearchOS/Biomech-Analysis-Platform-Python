from abc import abstractmethod
from typing import Any

from ResearchOS import PipelineObject

# Defaults should be of the same type as the expected values.
default_instance_attrs = {}
default_instance_attrs["current_analysis_id"] = None
default_instance_attrs["current_dataset_id"] = None
default_instance_attrs["project_path"] = None

default_abstract_attrs = {}

class Project(PipelineObject):
    """A project is a collection of analyses.
    Class-specific Attributes:
    1. current_analysis_id: The ID of the current analysis for this project.
    2. current_dataset_id: The ID of the current dataset for this project.
    3. project path: The root folder location of the project."""

    prefix: str = "PJ"
    _current_source_type_prefix = "US"
    _source_type_prefix = "US"

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Project)
    
    @abstractmethod
    def new_current(name: str = "New Project") -> "Project":
        """Create a new analysis and set it as the current analysis for the current project.
        Returns the project & analysis objects as a tuple."""
        from ResearchOS import Analysis
        pj = Project(name = name)
        an = Analysis(name = pj.name + "_Default_Analysis")
        pj.current_analysis_id = an.id
        return pj, an
    
    # TODO: Should I use __str__ or __repr__?
    def __str__(self):        
        if self.is_instance_object():
            return super().__str__(default_instance_attrs.keys(), self.__dict__)
        return super().__str__(default_abstract_attrs.keys(), self.__dict__)

    def __repr__(self):
        pass    
    
    #################### Start class-specific attributes ###################
    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""  
        attrs = {}
        if self.is_instance_object():
            attrs = default_instance_attrs  
        else:
            attrs = default_abstract_attrs    
        super().__init__(default_attrs = attrs, **kwargs)
    
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