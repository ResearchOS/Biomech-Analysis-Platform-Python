from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject

from abc import abstractmethod

class Process(PipelineObject):

    prefix = "PR"

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Process)

    #################### Start class-specific attributes ###################

    #################### Start Source objects ####################
    def get_analyses(self) -> list:
        """Return a list of analysis objects that belong to this process."""
        from src.ResearchOS.PipelineObjects.analysis import Analysis
        an_ids = self._get_all_source_object_ids(cls = Analysis)
        return [Analysis(id = an_id) for an_id in an_ids]
    
    #################### Start Target objects ####################
    def get_input_variable_ids(self) -> list:
        """Return a list of variable IDs that belong to this process."""
        from src.ResearchOS.variable import Variable
        return self._get_all_source_object_ids(cls = Variable)
    
    def get_output_variable_ids(self) -> list:
        """Return a list of variable IDs that belong to this process."""
        from src.ResearchOS.variable import Variable
        return self._get_all_target_object_ids(cls = Variable)
    
    def add_input_variable(self, var_id):
        """Add an input variable to the process."""
        # TODO: Need to add a mapping between variable ID and name in code.
        from src.ResearchOS.variable import Variable        
        self._add_source_object_id(var_id, cls = Variable)

    def add_output_variable(self, var_id):
        """Add an output variable to the process."""
        # TODO: Need to add a mapping between variable ID and name in code.
        from src.ResearchOS.variable import Variable        
        self._add_target_object_id(var_id, cls = Variable)

    def remove_input_variable(self, var_id):
        """Remove an input variable from the process."""
        from src.ResearchOS.variable import Variable        
        self._remove_source_object_id(var_id, cls = Variable)

    def remove_output_variable(self, var_id):
        """Remove an output variable from the process."""
        from src.ResearchOS.variable import Variable        
        self._remove_target_object_id(var_id, cls = Variable)


if __name__=="__main__":
    pr = Process()
    pr.add_input_variable(var = "id1")
    pr.add_output_variable(var = "id2")
    pr.assign_code(Process.square)
    pr.subset_data(gender == 'm')
    pr.run()

