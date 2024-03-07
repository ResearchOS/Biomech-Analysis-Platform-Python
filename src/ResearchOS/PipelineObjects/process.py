from typing import Any
from typing import Callable
import json, sys, os
import pickle
import importlib
from hashlib import sha256
from datetime import datetime, timezone
import logging, time

import networkx as nx

from ResearchOS.research_object import ResearchObject
from ResearchOS.variable import Variable
from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.PipelineObjects.subset import Subset
from ResearchOS.DataObjects.dataset import Dataset
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.code_inspector import get_returned_variable_names, get_input_variable_names
from ResearchOS.action import Action
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.process_runner import ProcessRunner


from inspect_locals import inspect_locals
import json

all_default_attrs = {}
all_default_attrs["is_matlab"] = False
all_default_attrs["mfolder"] = None
all_default_attrs["mfunc_name"] = None

all_default_attrs["method"] = None
all_default_attrs["level"] = None
all_default_attrs["input_vrs"] = {}
all_default_attrs["output_vrs"] = {}
all_default_attrs["vrs_source_pr"] = {}
all_default_attrs["subset_id"] = None

all_default_attrs["import_file_ext"] = None
all_default_attrs["import_file_vr_name"] = None

computer_specific_attr_names = ["mfolder"]

# log_stream = open("logfile_run_process.log", "w")

# Configure logging
logging.basicConfig(level=logging.DEBUG, filename = "logfile.log", filemode = "w", format = "%(asctime)s - %(levelname)s - %(message)s")

do_run = False


class Process(PipelineObject):

    prefix = "PR"
    _initialized = False

    def __init__(self, is_matlab: bool = all_default_attrs["is_matlab"], 
                 mfolder: str = all_default_attrs["mfolder"], 
                 mfunc_name: str = all_default_attrs["mfunc_name"], 
                 method: Callable = all_default_attrs["method"], 
                 level: type = all_default_attrs["level"], 
                 input_vrs: dict = all_default_attrs["input_vrs"], 
                 output_vrs: dict = all_default_attrs["output_vrs"], 
                 subset_id: str = all_default_attrs["subset_id"], 
                 import_file_ext: str = all_default_attrs["import_file_ext"], 
                 import_file_vr_name: str = all_default_attrs["import_file_vr_name"], 
                 vrs_source_pr: dict = all_default_attrs["vrs_source_pr"],
                 **kwargs) -> None:
        self.is_matlab = is_matlab
        self.mfolder = mfolder
        self.mfunc_name = mfunc_name
        self.method = method
        self.level = level
        self.input_vrs = input_vrs
        self.output_vrs = output_vrs
        self.subset_id = subset_id
        self.import_file_ext = import_file_ext
        self.import_file_vr_name = import_file_vr_name
        self.vrs_source_pr = vrs_source_pr
        super().__init__(**kwargs)

    ## mfunc_name (MATLAB function name) methods

    def validate_mfunc_name(self, mfunc_name: str, action: Action, default: Any) -> None:
        """Validate that the MATLAB function name is correct."""
        if mfunc_name == default:
            return
        self.validate_mfolder(self.mfolder, action, None)
        if not self.is_matlab and mfunc_name is None: 
            return
        if not isinstance(mfunc_name, str):
            raise ValueError("Function name must be a string!")
        if not str(mfunc_name).isidentifier():
            raise ValueError("Function name must be a valid variable name!")
        
    ## mfolder (MATLAB folder) methods
        
    def validate_mfolder(self, mfolder: str, action: Action, default: Any) -> None:
        """Validate that the MATLAB folder attribute is correct."""
        if mfolder == default:
            return
        if not self.is_matlab and mfolder is None:
            return
        if not isinstance(mfolder, str):
            raise ValueError("Path must be a string!")
        if not os.path.exists(mfolder):
            raise ValueError("Path must be a valid existing folder path!")
        
    ## method (Python method) methods
        
    def validate_method(self, method: Callable, action: Action, default: Any) -> None:
        """Validate that the Python method attribute is correct.
        
        Args:
            self
            method (Callable) : IDK what callable is
        
        Returns:
            None
        
        Raises:
            ValueError: incorrect inputted method format"""
        if method == default:
            return
        if method is None and self.is_matlab:
            return
        if not self.is_matlab and method is None:
            raise ValueError("Method cannot be None if is_matlab is False.")
        if not isinstance(method, Callable):
            raise ValueError("Method must be a callable function!")
        if method.__module__ not in sys.modules:
            raise ValueError("Method must be in an imported module!")

    def from_json_method(self, json_method: str, action: Action) -> Callable:
        """Convert a JSON string to a method.
        
        Args:
            self
            json_method (string) : JSON method to convert as a string
        
        Returns:
            Callable: IDK note add fancy linking thing once you know what a callable
            Returns None if the method name is not found (e.g. if code changed locations or something)"""
        method_name = json.loads(json_method)
        module_name, *attribute_path = method_name.split(".")
        if module_name not in sys.modules:
            module = importlib.import_module(module_name)
        attribute = module
        for attr in attribute_path:
            attribute = getattr(attribute, attr)
        return attribute

    def to_json_method(self, method: Callable, action: Action) -> str:
        """Convert a method to a JSON string.
        
        Args:
            self
            method (Callable) : python object representing code thats about to be run
        
        Returns:
            the method as a JSON string"""
        if method is None:
            return json.dumps(None)
        return json.dumps(method.__module__ + "." + method.__qualname__)
    
    ## level (Process level) methods

    def validate_level(self, level: type, action: Action, default: Any) -> None:
        """Validate that the level is correct."""
        if level == default:
            return
        if not isinstance(level, type):
            raise ValueError("Level must be a type!")
        
    def from_json_level(self, json_level: str, action: Action) -> type:
        """Convert a JSON string to a Process level.
        
        Args:
            self
            level (string) : IDK
            
        Returns:
            the JSON ''level'' as a type"""
        level = json.loads(json_level)
        classes = ResearchObjectHandler._get_subclasses(ResearchObject)
        for cls in classes:
            if hasattr(cls, "prefix") and cls.prefix == level:
                return cls

    def to_json_level(self, level: type, action: Action) -> str:
        """Convert a Process level to a JSON string."""
        if level is None:
            return json.dumps(None)
        return json.dumps(level.prefix)
    
    ## subset_id (Subset ID) methods
    
    def validate_subset_id(self, subset_id: str, action: Action, default: Any) -> None:
        """Validate that the subset ID is correct."""
        if subset_id == default:
            return
        if not ResearchObjectHandler.object_exists(subset_id, action):
            raise ValueError("Subset ID must reference an existing Subset.")
        
    ## input & output VRs methods
        
    def validate_input_vrs(self, inputs: dict, action: Action, default: Any) -> None:
        """Validate that the input variables are correct."""
        input_vr_names_in_code = []
        if not self.is_matlab:
            input_vr_names_in_code = get_input_variable_names(self.method)
        self._validate_vrs(inputs, input_vr_names_in_code, action, default)

    def validate_output_vrs(self, outputs: dict, action: Action, default: Any) -> None:
        """Validate that the output variables are correct."""
        output_vr_names_in_code = []
        if not self.is_matlab:
            output_vr_names_in_code = get_returned_variable_names(self.method)
        self._validate_vrs(outputs, output_vr_names_in_code, action, default)    

    def _validate_vrs(self, vr: dict, vr_names_in_code: list, action: Action, default: Any) -> None:
        """Validate that the input and output variables are correct. They should follow the same format.
        The format is a dictionary with the variable name as the key and the variable ID as the value."""
        if vr == default:
            return
        self.validate_method(self.method, action, None)
        if not isinstance(vr, dict):
            raise ValueError("Variables must be a dictionary.")
        for key, value in vr.items():
            if not isinstance(key, str):
                raise ValueError("Variable names in code must be strings.")
            if not str(key).isidentifier():
                raise ValueError("Variable names in code must be valid variable names.")
            if not isinstance(value, Variable):
                raise ValueError("Variable ID's must be Variable objects.")
            if not ResearchObjectHandler.object_exists(value.id, action):
                raise ValueError("Variable ID's must reference existing Variables.")
        if not self.is_matlab and not all([vr_name in vr_names_in_code for vr_name in vr.keys()]):
            raise ValueError("Output variables must be returned by the method.")
        
    ## import_file_ext
        
    def validate_import_file_ext(self, file_ext: str, action: Action, default: Any) -> None:
        if file_ext == default:
            return
        if not self.import_file_vr_name and file_ext is None:
            return
        if self.import_file_vr_name and file_ext is None:
            raise ValueError("File extension must be specified if import_file_vr_name is specified.")
        if not isinstance(file_ext, str):
            raise ValueError("File extension must be a string.")
        if not file_ext.startswith("."):
            raise ValueError("File extension must start with a period.")
        
    ## import_file_vr_name
        
    def validate_import_file_vr_name(self, vr_name: str, action: Action, default: Any) -> None:
        if vr_name == default:
            return
        self.validate_input_vrs(self.input_vrs, action, None)
        if not isinstance(vr_name, str):
            raise ValueError("Variable name must be a string.")
        if not str(vr_name).isidentifier():
            raise ValueError("Variable name must be a valid variable name.")
        if vr_name not in self.input_vrs:
            raise ValueError("Variable name must be a valid input variable name.")
        
    def from_json_input_vrs(self, input_vrs: str, action: Action) -> dict:
        """Convert a JSON string to a dictionary of input variables."""
        input_vr_ids_dict = json.loads(input_vrs)
        input_vrs_dict = {}
        for name, vr_id in input_vr_ids_dict.items():
            vr = Variable(id = vr_id, action = action)
            input_vrs_dict[name] = vr
        return input_vrs_dict
    
    def to_json_input_vrs(self, input_vrs: dict, action: Action) -> str:
        """Convert a dictionary of input variables to a JSON string."""     
        return json.dumps({key: value.id for key, value in input_vrs.items()})
    
    def from_json_output_vrs(self, output_vrs: str, action: Action) -> dict:
        """Convert a JSON string to a dictionary of output variables."""
        output_vr_ids_dict = json.loads(output_vrs)
        output_vrs_dict = {}
        for name, vr_id in output_vr_ids_dict.items():
            vr = Variable(id = vr_id, action = action)
            output_vrs_dict[name] = vr
        return output_vrs_dict
    
    # vrs_source_pr methods
    def validate_vrs_source_pr(self, vrs_source_pr: dict, action: Action, default: Any) -> None:
        """Validate that the source process for the input variables is correct."""
        if vrs_source_pr == default:
            return
        if not isinstance(vrs_source_pr, dict):
            raise ValueError("Source process must be a dictionary.")
        for key, value in vrs_source_pr.items():
            if not isinstance(key, str):
                raise ValueError("Variable names in code must be strings.")
            if not str(key).isidentifier():
                raise ValueError("Variable names in code must be valid variable names.")
            if not isinstance(value, Process):
                raise ValueError("Source process must be a Process object.")
            if not ResearchObjectHandler.object_exists(value.id, action):
                raise ValueError("Source process must reference existing Process.")
            
    def from_json_vrs_source_pr(self, vrs_source_pr: str, action: Action) -> dict:
        """Convert a JSON string to a dictionary of source processes for the input variables."""
        vrs_source_pr_ids_dict = json.loads(vrs_source_pr)
        vrs_source_pr_dict = {}
        for name, pr_id in vrs_source_pr_ids_dict.items():
            pr = Process(id = pr_id, action = action)
            vrs_source_pr_dict[name] = pr
        return vrs_source_pr_dict
    
    def to_json_vrs_source_pr(self, vrs_source_pr: dict, action: Action) -> str:
        """Convert a dictionary of source processes for the input variables to a JSON string."""     
        return json.dumps({key: value.id for key, value in vrs_source_pr.items()})
    
    def to_json_output_vrs(self, output_vrs: dict, action: Action) -> str:
        """Convert a dictionary of output variables to a JSON string."""
        return json.dumps({key: value.id for key, value in output_vrs.items()})
    
    def set_input_vrs(self, **kwargs) -> None:
        """Convenience function to set the input variables with named variables rather than a dict."""
        self.__setattr__("input_vrs", kwargs)

    def set_output_vrs(self, **kwargs) -> None:
        """Convenience function to set the output variables with named variables rather than a dict."""
        self.__setattr__("output_vrs", kwargs)

    def set_vrs_source_pr(self, **kwargs) -> None:
        """Convenience function to set the source process for the input variables with named variables rather than a dict."""
        self.__setattr__("vrs_source_pr", kwargs)

    def run(self, force_redo: bool = False) -> None:
        """Execute the attached method.
        kwargs are the input VR's."""      
        action = Action(name = f"Running {self.mfunc_name} on {self.level.__name__}s.")  
        ds = Dataset(id = self.get_dataset_id(), action = action)   
        ds_defaults = DefaultAttrs(ds).default_attrs
        # Validate the dataset's addresses.
        addresses_valid = ds.validate_addresses(ds.addresses, action, ds_defaults["addresses"])        
        if not addresses_valid:
            raise ValueError("The dataset's addresses are not valid.")

        defaults = DefaultAttrs(self).default_attrs
        default_attrs = defaults
        
        # 1. Validate that the level & method have been properly set.
        self.validate_method(self.method, action, default = default_attrs["method"])
        self.validate_level(self.level, action, default = default_attrs["level"])

        # TODO: Fix this to work with MATLAB.
        if not self.is_matlab:
            output_var_names_in_code = get_returned_variable_names(self.method)

        # 2. Validate that the input & output variables have been properly set.
        self.validate_input_vrs(self.input_vrs, action, default = default_attrs["input_vrs"])
        self.validate_output_vrs(self.output_vrs, action, default = default_attrs["output_vrs"])

        # 3. Validate that the subsets have been properly set.
        self.validate_subset_id(self.subset_id, action, default = default_attrs["subset_id"])

        # 4. Validate vrs_source_pr
        self.validate_vrs_source_pr(self.vrs_source_pr, action, default = default_attrs["vrs_source_pr"])

        if self.is_matlab:
            self.validate_mfunc_name(self.mfunc_name, action, default = default_attrs["mfunc_name"])
            self.validate_mfolder(self.mfolder, action, default = default_attrs["mfolder"])
            self.validate_import_file_ext(self.import_file_ext, action, default = default_attrs["import_file_ext"])
            self.validate_import_file_vr_name(self.import_file_vr_name, action, default = default_attrs["import_file_vr_name"])

        schema_id = self.get_current_schema_id(ds.id)

        matlab_loaded = True
        if "matlab" not in sys.modules:
            matlab_loaded = False
            try:            
                print("Importing MATLAB.")
                import matlab.engine
                eng = matlab.engine.start_matlab()
                matlab_loaded = True
            except:
                print("Failed to import MATLB.")
                matlab_loaded = False                
                
        # 4. Run the method.
        # Get the subset of the data.
        subset = Subset(id = self.subset_id, action = action)
        # subset_graph = subset.get_subset()
        subset_graph = nx.MultiDiGraph()
        subset_graph.add_edges_from(ds.addresses)

        level_node_ids = [node for node in subset_graph if node.startswith(self.level.prefix)]
        name_attr_id = ResearchObjectHandler._get_attr_id("name")
        sqlquery_raw = "SELECT object_id, attr_value FROM simple_attributes WHERE attr_id = ? AND object_id LIKE ?"
        sqlquery = sql_order_result(action, sqlquery_raw, ["object_id"], single = True, user = True, computer = False)
        params = (name_attr_id, f"{self.level.prefix}%")
        level_nodes_ids_names = action.conn.cursor().execute(sqlquery, params).fetchall()
        level_nodes_ids_names.sort(key = lambda x: x[1])
        level_node_ids_sorted = [row[0] for row in level_nodes_ids_names if row[0] in level_node_ids]
        # Iterate over each data object at this level (e.g. all ros.Trial objects in the subset.)
        schema = ds.schema
        schema_graph = nx.MultiDiGraph(schema)
        schema_order = list(nx.topological_sort(schema_graph))

        process_run_file_path = os.path.join(config.process_run_tmp_folder, config.process_run_file_name)
        
        pool = SQLiteConnectionPool()
        process_runner = ProcessRunner(self, action, schema_id, schema_order, ds, subset_graph, matlab_loaded, eng, force_redo)
        for node_id in level_node_ids_sorted:            
            process_runner.run_node(node_id)
            
        for vr_name, vr in self.output_vrs.items():
            print(f"Saved VR {vr_name} (VR: {vr.id}).")

        pool.return_connection(action.conn)
