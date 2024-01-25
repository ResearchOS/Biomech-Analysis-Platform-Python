import ResearchOS as ros
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

    def from_json_method(self):
        pass

    def to_json_method(self):
        pass

    def from_json_level(self):
        pass

    def to_json_level(self):
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
        # 1. Validate that the level & method have been properly set.
        self.validate_method(self.method)
        self.validate_level(self.level)

        # 2. Validate that the input & output variables have been properly set.
        self.validate_input_variables()
        self.validate_output_variables()

        # 3. Validate that the subsets have been properly set.
        self.validate_subset()

        # 4. Run the method.
        # Get the subset of the data.
        subset_dict = {} # Comes from Subset object.
        # Get data schema
        us = ros.User(id = ros.User.get_current_user_object_id())
        pj = ros.Project(id = us.current_project_id)
        ds = ros.Dataset(id = pj.current_dataset_id)
        schema = ds.schema
        # Preserves the hierarchy order, but only includes levels needed for this method.
        curr_schema = [sch for sch in schema if sch in self.level]
        self._run_index = -1 # Tells the run method which index we are on.
        self._current_schema = [None for _ in range(len(curr_schema))] # Initialize with None.
        self.run_recurse(curr_schema)

    def run_recurse(self, full_schema: list[type]) -> None:
        """Run the method, looping recursively."""        
        self._run_index +=1
        for sch in full_schema:
            # If index is not at the lowest level, recurse.
            self._current_schema[self._run_index] = sch
            if self._run_index < len(full_schema) - 1:                
                self.run_recurse(full_schema)
                continue
            # If index is at the lowest level, run the method.
            # Get the input variables.            
            self.method() 
        self._current_schema[self._run_index] = None # Reset
        self._run_index -=1
            


if __name__=="__main__":
    pr = Process()
    pr.add_input_variable(var = "id1")
    pr.add_output_variable(var = "id2")
    pr.assign_code(Process.square)
    pr.subset_data(gender == 'm')
    pr.run()

