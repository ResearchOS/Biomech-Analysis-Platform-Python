from ResearchOS import PipelineObject
from typing import Callable

from abc import abstractmethod

default_attrs = {}
default_attrs["method"] = None
default_attrs["level"] = None


class Process(PipelineObject):

    prefix = "PR"

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Process)

    #################### Start class-specific attributes ###################
    def validate_method(self, method: Callable) -> None:
        pass

    def validate_level(self, level: type) -> None:
        pass

    def json_translate_method(self):
        pass

    def json_translate_level(self):
        pass

    #################### Start Source objects ####################
    def get_analyses(self) -> list:
        """Return a list of analysis objects that belong to this process."""
        from ResearchOS import Analysis
        an_ids = self._get_all_source_object_ids(cls = Analysis)
        return [Analysis(id = an_id) for an_id in an_ids]
    
    #################### Start Target objects ####################

    #################### Start class-specific methods ###################
    def get_input_variables(self) -> list:
        """Return a list of variable IDs that belong to this process."""
        from ResearchOS import Variable
        vr_ids = self._get_all_source_object_ids(cls = Variable)
        return self._gen_obj_or_none(vr_ids, Variable)
    
    def get_output_variables(self) -> list:
        """Return a list of variable IDs that belong to this process."""
        from ResearchOS import Variable
        vr_ids = self._get_all_target_object_ids(cls = Variable)
        return self._gen_obj_or_none(vr_ids, Variable)
    
    def get_subsets(self) -> list:
        """Return a list of subset IDs that belong to this process."""
        from ResearchOS import Subset
        ss_ids = self._get_all_target_object_ids(cls = Subset)
        return self._gen_obj_or_none(ss_ids, Subset)
    
    def add_input_variable_id(self, var_id):
        """Add an input variable to the process."""
        # TODO: Need to add a mapping between variable ID and name in code.
        from ResearchOS import Variable        
        self._add_source_object_id(var_id, cls = Variable)

    def add_output_variable_id(self, var_id):
        """Add an output variable to the process."""
        # TODO: Need to add a mapping between variable ID and name in code.
        from ResearchOS import Variable        
        self._add_target_object_id(var_id, cls = Variable)

    def remove_input_variable_id(self, var_id):
        """Remove an input variable from the process."""
        from ResearchOS import Variable        
        self._remove_source_object_id(var_id, cls = Variable)

    def remove_output_variable_id(self, var_id):
        """Remove an output variable from the process."""
        from ResearchOS import Variable        
        self._remove_target_object_id(var_id, cls = Variable)

    def add_subset_id(self, ss_id):
        """Add a subset to the process."""
        from ResearchOS import Subset
        self._add_target_object_id(ss_id, cls = Subset)

    def remove_subset_id(self, ss_id):
        """Remove a subset from the process."""
        from ResearchOS import Subset
        self._remove_target_object_id(ss_id, cls = Subset)

    def run_method(self) -> None:
        """Execute the attached method."""
        pass


if __name__=="__main__":
    pr = Process()
    pr.add_input_variable(var = "id1")
    pr.add_output_variable(var = "id2")
    pr.assign_code(Process.square)
    pr.subset_data(gender == 'm')
    pr.run()

