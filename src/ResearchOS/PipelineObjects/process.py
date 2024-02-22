from typing import Any
from typing import Callable
import json, sys, os
import importlib

import networkx as nx

from ResearchOS.research_object import ResearchObject
from ResearchOS.variable import Variable
from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.PipelineObjects.subset import Subset
from ResearchOS.DataObjects.dataset import Dataset
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.code_inspector import get_returned_variable_names, get_input_variable_names
from ResearchOS.action import Action

all_default_attrs = {}
all_default_attrs["method"] = None
all_default_attrs["level"] = None
all_default_attrs["input_vrs"] = {}
all_default_attrs["output_vrs"] = {}
all_default_attrs["subset_id"] = None
all_default_attrs["is_matlab"] = False
all_default_attrs["mfolder"] = None
all_default_attrs["mfunc_name"] = None

complex_attrs_list = []

try:
    import matlab.engine
    eng = matlab.engine.start_matlab()
except:
    pass


class Process(PipelineObject):

    prefix = "PR"

    ## mfunc_name (MATLAB function name) methods

    def validate_mfunc_name(self, mfunc_name: str) -> None:
        self.validate_mfolder(self.mfolder)
        if not self.is_matlab and mfunc_name is None: 
            return
        if not isinstance(mfunc_name, str):
            raise ValueError("Function name must be a string!")
        if not str(mfunc_name).isidentifier():
            raise ValueError("Function name must be a valid variable name!")
        if not os.path.exists(os.path.join(self.mfolder, mfunc_name + ".m")):
            raise ValueError("Function name must reference an existing MATLAB function in the specified folder.")
        
    ## mfolder (MATLAB folder) methods
        
    def validate_mfolder(self, mfolder: str) -> None:
        if not self.is_matlab and mfolder is None:
            return
        if not self.is_matlab:
            raise ValueError("mfolder must be None if is_matlab is False.")
        if not isinstance(mfolder, str):
            raise ValueError("Path must be a string!")
        if not os.path.exists(mfolder):
            raise ValueError("Path must be a valid existing folder path!")
        
    ## method (Python method) methods
        
    def validate_method(self, method: Callable) -> None:
        if method is None and self.is_matlab:
            return
        if not self.is_matlab:
            raise ValueError("Method must be None if is_matlab is False.")
        if not isinstance(method, Callable):
            raise ValueError("Method must be a callable function!")
        if method.__module__ not in sys.modules:
            raise ValueError("Method must be in an imported module!")

    def from_json_method(self, json_method: str) -> Callable:
        """Convert a JSON string to a method.
        Returns None if the method name is not found (e.g. if code changed locations or something)"""
        method_name = json.loads(json_method)
        module_name, *attribute_path = method_name.split(".")        
        module = importlib.import_module(module_name)
        attribute = module
        for attr in attribute_path:
            attribute = getattr(attribute, attr)
        return attribute

    def to_json_method(self, method: Callable) -> str:
        """Convert a method to a JSON string."""
        if method is None:
            return json.dumps(None)
        return json.dumps(method.__module__ + "." + method.__qualname__)
    
    ## level (Process level) methods

    def validate_level(self, level: type) -> None:
        if not isinstance(level, type):
            raise ValueError("Level must be a type!")
        
    def from_json_level(self, level: str) -> type:
        """Convert a JSON string to a Process level."""
        classes = ResearchObjectHandler._get_subclasses(ResearchObject)
        for cls in classes:
            if hasattr(cls, "prefix") and cls.prefix == level:
                return cls

    def to_json_level(self, level: type) -> str:
        """Convert a Process level to a JSON string."""
        return json.dumps(level.prefix)
    
    ## subset_id (Subset ID) methods
    
    def validate_subset_id(self, subset_id: str) -> None:
        """Validate that the subset ID is correct."""
        if not ResearchObjectHandler.object_exists(subset_id):
            raise ValueError("Subset ID must reference an existing Subset.")
        
    ## input & output VRs methods
        
    def validate_input_vrs(self, inputs: dict) -> None:
        """Validate that the input variables are correct."""        
        if not self.is_matlab:
            input_vr_names_in_code = get_input_variable_names(self.method)
        self._validate_vrs(inputs, input_vr_names_in_code)

    def validate_output_vrs(self, outputs: dict) -> None:
        """Validate that the output variables are correct."""        
        if not self.is_matlab:
            output_vr_names_in_code = get_returned_variable_names(self.method)
        self._validate_vrs(outputs, output_vr_names_in_code)    

    def _validate_vrs(self, vr: dict, vr_names_in_code: list) -> None:
        """Validate that the input and output variables are correct. They should follow the same format.
        The format is a dictionary with the variable name as the key and the variable ID as the value."""       
        self.validate_method(self.method) 
        if not isinstance(vr, dict):
            raise ValueError("Variables must be a dictionary.")
        for key, value in vr.items():
            if not isinstance(key, str):
                raise ValueError("Variable names in code must be strings.")
            if not str(key).isidentifier():
                raise ValueError("Variable names in code must be valid variable names.")
            if not isinstance(value, Variable):
                raise ValueError("Variable ID's must be Variable objects.")
            if not ResearchObjectHandler.object_exists(value.id):
                raise ValueError("Variable ID's must reference existing Variables.")
        if not self.is_matlab and not all([vr_name in vr_names_in_code for vr_name in vr.keys()]):
            raise ValueError("Output variables must be returned by the method.")
        
    def from_json_input_vrs(self, input_vrs: str) -> dict:
        """Convert a JSON string to a dictionary of input variables."""
        input_vrs_dict = json.loads(input_vrs)
        return {key: Variable(id = value) for key, value in input_vrs_dict.items()}
    
    def to_json_input_vrs(self, input_vrs: dict) -> str:
        """Convert a dictionary of input variables to a JSON string."""     
        return json.dumps({key: value.id for key, value in input_vrs.items()})
    
    def from_json_output_vrs(self, output_vrs: str) -> dict:
        """Convert a JSON string to a dictionary of output variables."""
        output_vrs_dict = json.loads(output_vrs)
        return {key: Variable(id = value) for key, value in output_vrs_dict.items()}
    
    def to_json_output_vrs(self, output_vrs: dict) -> str:
        """Convert a dictionary of output variables to a JSON string."""
        return json.dumps({key: value.id for key, value in output_vrs.items()})
    
    def set_input_vrs(self, **kwargs) -> None:
        """Convenience function to set the input variables with named variables rather than a dict."""
        action = Action(name = "set_input_vrs")
        self.__setattr__("input_vrs", kwargs, action = action)

    def set_output_vrs(self, **kwargs) -> None:
        """Convenience function to set the output variables with named variables rather than a dict."""
        action = Action(name = "set_output_vrs")
        self.__setattr__("output_vrs", kwargs, action = action)

    def run(self) -> None:
        """Execute the attached method.
        kwargs are the input VR's."""
        ds = Dataset(id = self.get_dataset_id())
        # 1. Validate that the level & method have been properly set.
        self.validate_method(self.method)
        self.validate_level(self.level)

        # TODO: Fix this to work with MATLAB.
        if not self.is_matlab:
            output_var_names_in_code = get_returned_variable_names(self.method)

        # 2. Validate that the input & output variables have been properly set.
        self.validate_input_vrs(self.input_vrs)
        self.validate_output_vrs(self.output_vrs)

        # 3. Validate that the subsets have been properly set.
        self.validate_subset_id(self.subset_id)

        # 4. Run the method.
        # Get the subset of the data.
        subset_graph = Subset(id = self.subset_id).get_subset()

        action = Action(name = f"Running {self.mfunc_name} on {self.level.__name__}s.")

        # Do the setup for MATLAB.
        if self.is_matlab:
            eng.addpath(self.mfolder, nargout=0)

        level_nodes = sorted([node for node in subset_graph if isinstance(node, self.level)], key = lambda x: x.name)
        # Iterate over each data object at this level (e.g. all ros.Trial objects in the subset.)
        schema = ds.schema
        schema_graph = nx.MultiDiGraph(schema)
        schema_order = list(nx.topological_sort(schema_graph))
        for node in level_nodes:
            # Get the values for the input variables for this DataObject node.
            print(f"Running {self.mfunc_name} on {node.name}.")
            vr_values_in = {}
            anc_nodes = nx.ancestors(subset_graph, node)
            node_lineage = [node] + [anc_node for anc_node in anc_nodes]
            for var_name_in_code, vr in self.input_vrs.items():
                vr_found = False
                if vr.hard_coded_value is not None:
                    vr_values_in[var_name_in_code] = vr.hard_coded_value
                    vr_found = True                
                for curr_node in node_lineage:
                    if hasattr(curr_node, vr.name):
                        vr_values_in[var_name_in_code] = getattr(curr_node, vr.name)
                        vr_found = True
                        break
                if not vr_found:
                    raise ValueError(f"Variable {vr.name} ({vr.id}) not found in __dict__ of {node}.")
                
            # Get the lineage so I can get the file path.
            ordered_levels = []
            for level in schema_order:
                ordered_levels.append([n for n in node_lineage if isinstance(n, level)])
            data_path = ds.dataset_path
            for level in ordered_levels[1:]:
                data_path = os.path.join(data_path, level[0].name)
            if "c3dFilePath" in vr_values_in:
                vr_values_in["c3dFilePath"] = data_path + ".c3d"

            # NOTE: For now, assuming that there is only one return statement in the entire method.
            if self.is_matlab:
                vr_vals_in = list(vr_values_in.values())
                fcn = getattr(eng, self.mfunc_name)                
                vr_values_out = fcn(*vr_vals_in, nargout=len(self.output_vrs))
            else:
                vr_values_out = self.method(**vr_values_in) # Ensure that the method returns a tuple.
            if not isinstance(vr_values_out, tuple):
                vr_values_out = (vr_values_out,)
            if len(vr_values_out) != len(self.output_vrs):
                raise ValueError("The number of variables returned by the method must match the number of output variables registered with this Process instance.")
            if not all(vr in self.output_vrs for vr in output_var_names_in_code):
                raise ValueError("All of the variable names returned by this method must have been previously registered with this Process instance.")            

            # Set the output variables for this DataObject node.
            idx = -1 # For MATLAB. Requires that the args are in the proper order.
            for vr_name, vr in self.output_vrs.items():
                if not self.is_matlab:
                    idx = output_var_names_in_code.index(vr_name) # Ensure I'm pulling the right VR name because the order of the VR's coming out, and the order in the output_vrs dict are probably different.
                else:
                    idx += 1                
                self.__setattr__(node, vr_name, vr_values_out[idx], action = action)
                print(f"In {node.name} ({node.id}): Saved VR {vr_name} (VR: {vr.id}).")

        if self.is_matlab:
            eng.rmpath(self.mfolder, nargout=0)   

