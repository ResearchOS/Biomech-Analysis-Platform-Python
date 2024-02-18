from typing import Any
from typing import Callable
import json

from ResearchOS.research_object import ResearchObject
from ResearchOS.variable import Variable
from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.PipelineObjects.subset import Subset
from ResearchOS.DataObjects.dataset import Dataset
from ResearchOS.PipelineObjects.project import Project
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.db_connection_factory import DBConnectionFactory
from ResearchOS.current_user import CurrentUser
from ResearchOS.code_inspector import get_returned_variable_names

all_default_attrs = {}
all_default_attrs["method"] = None
all_default_attrs["level"] = None
all_default_attrs["input_vr"] = []
all_default_attrs["output_vr"] = []

complex_attrs_list = []


class Process(PipelineObject):

    prefix = "PR"

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

    #################### Start class-specific attributes ###################
    def validate_method(self, method: Callable) -> None:
        if not isinstance(method, Callable):
            raise ValueError("Method must be a callable function!")
        if method not in globals().values():
            raise ValueError("Method must be a global function!")

    def validate_level(self, level: type) -> None:
        if not isinstance(level, type):
            raise ValueError("Level must be a type!")
        
    def validate_input_vr(self, inputs: dict) -> None:
        """Validate that the input variables are correct."""
        self._validate_vr(self, inputs)

    def validate_output_vr(self, outputs: dict) -> None:
        """Validate that the output variables are correct."""
        self._validate_vr(self, outputs)

    def _validate_vr(self, vr: dict) -> None:
        """Validate that the input and output variables are correct. They should follow the same format.
        The format is a dictionary with the variable name as the key and the variable ID as the value."""
        if not isinstance(vr, dict):
            raise ValueError("Variables must be a dictionary.")
        for key, value in vr.items():
            if not isinstance(key, str):
                raise ValueError("Variable names in code must be strings.")
            if not str(key).isidentifier():
                raise ValueError("Variable names in code must be valid variable names.")
            if not ResearchObjectHandler.object_exists(value):
                raise ValueError("Variable ID's must reference existing Variables.")

    def from_json_method(self, json_method: str) -> Callable:
        """Convert a JSON string to a method.
        Returns None if the method name is not found (e.g. if code changed locations or something)"""
        method_name = json.loads(json_method)
        if method_name in globals():
            method = globals()[method_name]
        else:
           print(f"Method {method_name} not found in globals.")
           method = None
        return method

    def to_json_method(self, method: Callable) -> str:
        """Convert a method to a JSON string."""
        return json.dumps(method.__name__)

    def from_json_level(self, level: str) -> type:
        """Convert a JSON string to a Process level."""
        classes = ResearchObjectHandler._get_subclasses(ResearchObject)
        for cls in classes:
            if hasattr(cls, "prefix") and cls.prefix == level:
                return cls

    def to_json_level(self, level: type) -> str:
        """Convert a Process level to a JSON string."""
        return json.dumps(level.prefix)

    #################### Start class-specific methods ###################
    def get_input_variables(self) -> list:
        """Return a list of variable IDs that belong to this process."""        
        vr_ids = self._get_all_source_object_ids(cls = Variable)
        return self._gen_obj_or_none(vr_ids, Variable)
    
    def get_output_variables(self) -> list:
        """Return a list of variable IDs that belong to this process."""        
        vr_ids = self._get_all_target_object_ids(cls = Variable)
        return self._gen_obj_or_none(vr_ids, Variable)
    
    def get_subsets(self) -> list:
        """Return a list of subset IDs that belong to this process."""        
        ss_ids = self._get_all_target_object_ids(cls = Subset)
        return self._gen_obj_or_none(ss_ids, Subset)
    
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
        self.validate_input_vr()
        self.validate_output_vr()

        # 3. Validate that the subsets have been properly set.
        self.validate_subset()

        # 4. Run the method.
        # Get the subset of the data.
        subset_dict = {} # Comes from Subset object.
        # Get data schema
        conn = DBConnectionFactory().create_db_connection().conn
        us = CurrentUser(conn).get_current_user_id()
        pj = Project(id = us.current_project_id)
        ds = Dataset(id = pj.current_dataset_id)
        # Preserves the hierarchy order, but only includes levels needed for this method.
        curr_schema = [sch for sch in ds.schema if sch in self.level]
        self._run_index = -1 # Tells the run method which index we are on.
        self._current_schema = [None for _ in range(len(curr_schema))] # Initialize with None.
        self.run_recurse(curr_schema)
        self.output_vr_names_in_code = get_returned_variable_names(self.method)

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
            # 1. Get the input variables.
            input_vars_name_value = {} # Dict: var_name: var_value
            for vr in self.input_vr:
                input_vars_name_value[vr] = vr.get_value(sch)
            outputs = self.method(**input_vars_name_value)
            if not isinstance(outputs, tuple):
                raise ValueError("Method must return a tuple of outputs.")
            if len(outputs) != len(self.output_vr_names_in_code):
                raise ValueError("Method must return the same number of outputs as output variables.")
            for idx, output in enumerate(outputs):
                name_in_code = self.output_vr_names_in_code[idx]
                vr = Variable(id = name_in_code, address = sch)
                vr.value = output
        self._current_schema[self._run_index] = None # Reset
        self._run_index -=1
            


if __name__=="__main__":
    pr = Process()

