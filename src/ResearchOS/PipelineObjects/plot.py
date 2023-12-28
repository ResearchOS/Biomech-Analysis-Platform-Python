from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject

class Plot(PipelineObject):
    
        prefix = "PL"    

        # TODO: Plot name/other metadata for saving.
        # TODO: For variables, need to allow ability to specify which analysis to pull from.
    
        #################### Start class-specific attributes ###################

        #################### Start Source objects ####################
        def get_projects(self) -> list:
            """Return a list of project objects that belong to this plot."""
            from src.ResearchOS.PipelineObjects.project import Project
            pj_ids = self._get_all_source_object_ids(cls = Project)
            return [Project(id = pj_id) for pj_id in pj_ids]
        
        #################### Start Target objects ####################
        def get_variable_ids(self) -> list:
            """Return a list of variable IDs that belong to this plot."""
            from src.ResearchOS.variable import Variable
            return self._get_all_target_object_ids(cls = Variable)
        
        def add_variable_id(self, variable_id: str):
            """Add a variable to the plot."""
            # TODO: Need to add a mapping between variable ID and name in code.
            from src.ResearchOS.variable import Variable        
            self._add_target_object_id(variable_id, cls = Variable)

        def remove_variable_id(self, variable_id: str):
            """Remove a variable from the plot."""
            from src.ResearchOS.variable import Variable        
            self._remove_target_object_id(variable_id, cls = Variable)