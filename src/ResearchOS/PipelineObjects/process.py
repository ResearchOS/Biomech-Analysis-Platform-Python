from typing import Any
from typing import Callable
import json

import ResearchOS as ros
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.db_connection_factory import DBConnectionFactory

all_default_attrs = {}
all_default_attrs["method"] = None
all_default_attrs["level"] = None

complex_attrs_list = []


class Process(ros.PipelineObject):

    prefix = "PR"

    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        super().__init__(all_default_attrs, **kwargs)

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""
        if name == "vr":
            raise ValueError("The attribute 'vr' is not allowed to be set for Pipeline Objects.")
        else:
            ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)

    def load(self) -> None:
        """Load the dataset-specific attributes from the database in an attribute-specific way."""
        ros.PipelineObject.load(self) # Load the attributes specific to it being a PipelineObject.

    #################### Start class-specific attributes ###################
    def validate_method(self, method: Callable) -> None:
        if not isinstance(method, Callable):
            raise ValueError("Method must be a callable function!")

    def validate_level(self, level: type) -> None:
        if not isinstance(level, type):
            raise ValueError("Level must be a type!")

    def from_json_method(self, json_method: str) -> Callable:
        """Convert a JSON string to a method.
        Returns None if the method name is not found (e.g. if code changed locations or something)"""
        method_name = json.loads(json_method)
        if method_name in globals():
            method = globals()[method_name]
        else:
           print(f"Method {method_name} not found in globals.")
           method = None

    def to_json_method(self, method: Callable) -> str:
        """Convert a method to a JSON string."""
        return json.dumps(method.__name__)

    def from_json_level(self, level: str) -> type:
        """Convert a JSON string to a Process level."""
        classes = ResearchObjectHandler._get_subclasses(ros.ResearchObject)
        for cls in classes:
            if hasattr(cls, "prefix") and cls.prefix == level:
                return cls

    def to_json_level(self, level: type) -> str:
        """Convert a Process level to a JSON string."""
        return json.dumps(level.prefix)

    #################### Start class-specific methods ###################
    def get_input_variables(self) -> list:
        """Return a list of variable IDs that belong to this process."""        
        vr_ids = self._get_all_source_object_ids(cls = ros.Variable)
        return self._gen_obj_or_none(vr_ids, ros.Variable)
    
    def get_output_variables(self) -> list:
        """Return a list of variable IDs that belong to this process."""        
        vr_ids = self._get_all_target_object_ids(cls = ros.Variable)
        return self._gen_obj_or_none(vr_ids, ros.Variable)
    
    def get_subsets(self) -> list:
        """Return a list of subset IDs that belong to this process."""        
        ss_ids = self._get_all_target_object_ids(cls = ros.Subset)
        return self._gen_obj_or_none(ss_ids, ros.Subset)
    
    def add_input_variable_id(self, var_id):
        """Add an input variable to the process."""
        # TODO: Need to add a mapping between variable ID and name in code.        
        self._add_source_object_id(var_id)

    def add_output_variable_id(self, var_id):
        """Add an output variable to the process."""
        # TODO: Need to add a mapping between variable ID and name in code.        
        self._add_target_object_id(var_id)

    def remove_input_variable_id(self, var_id):
        """Remove an input variable from the process."""        
        self._remove_source_object_id(var_id)

    def remove_output_variable_id(self, var_id):
        """Remove an output variable from the process."""        
        self._remove_target_object_id(var_id)

    def add_subset_id(self, ss_id):
        """Add a subset to the process."""        
        self._add_target_object_id(ss_id)

    def remove_subset_id(self, ss_id):
        """Remove a subset from the process."""        
        self._remove_target_object_id(ss_id)

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

