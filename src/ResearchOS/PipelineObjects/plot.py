from ResearchOS.PipelineObjects.pipeline_object import PipelineObject

from abc import abstractmethod

default_attrs = {}

class Plot(PipelineObject):
    
        prefix = "PL"    

        # TODO: Plot name/other metadata for saving.
        # TODO: For variables, need to allow ability to specify which analysis to pull from.

        @abstractmethod
        def get_all_ids() -> list[str]:
            return super().get_all_ids(Plot)
        
        def __init__(self, **kwargs):
            """Initialize the attributes that are required by ResearchOS.
            Other attributes can be added & modified later."""
            super().__init__(default_attrs, **kwargs)
    
        #################### Start class-specific attributes ###################

        #################### Start Source objects ####################
        def get_projects(self) -> list:
            """Return a list of project objects that belong to this plot."""
            from ResearchOS import Project
            pj_ids = self._get_all_source_object_ids(cls = Project)
            return [Project(id = pj_id) for pj_id in pj_ids]
        
        #################### Start Target objects ####################
        def get_variable_ids(self) -> list:
            """Return a list of variable IDs that belong to this plot."""
            from ResearchOS import Variable
            return self._get_all_target_object_ids(cls = Variable)
        
        def add_variable_id(self, variable_id: str):
            """Add a variable to the plot."""
            # TODO: Need to add a mapping between variable ID and name in code.
            from ResearchOS import Variable        
            self._add_target_object_id(variable_id, cls = Variable)

        def remove_variable_id(self, variable_id: str):
            """Remove a variable from the plot."""
            from ResearchOS import Variable        
            self._remove_target_object_id(variable_id, cls = Variable)