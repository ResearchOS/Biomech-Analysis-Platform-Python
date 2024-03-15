from typing import Any
from typing import Callable
import json, sys, os
import importlib, logging
from copy import deepcopy

import networkx as nx

from ResearchOS.research_object import ResearchObject
from ResearchOS.variable import Variable
from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.PipelineObjects.subset import Subset
from ResearchOS.DataObjects.dataset import Dataset
from ResearchOS.PipelineObjects.logsheet import Logsheet
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.code_inspector import get_returned_variable_names, get_input_variable_names
from ResearchOS.action import Action
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.process_runner import ProcessRunner
from ResearchOS.Digraph.rodigraph import ResearchObjectDigraph

all_default_attrs = {}
# For MATLAB
all_default_attrs["is_matlab"] = False
all_default_attrs["mfolder"] = None
all_default_attrs["mfunc_name"] = None

# Main attributes
all_default_attrs["method"] = None
all_default_attrs["level"] = None
all_default_attrs["input_vrs"] = {}
all_default_attrs["output_vrs"] = {}
all_default_attrs["vrs_source_pr"] = {}
all_default_attrs["subset_id"] = None

# For import
all_default_attrs["import_file_ext"] = None
all_default_attrs["import_file_vr_name"] = None

# For including other Data Object attributes from the node lineage in the input variables.
# For example, if a Process is run on a Trial, and one of the inputs needs to be the Subject's name.
# Then, "data_object_level_attr" would be "{ros.Subject: 'name'}"
# NOTE: This is always the last input variable(s), in the order of the input variables dict.
# all_default_attrs["data_object_level_attr"] = {}

# For static lookup trial
all_default_attrs["lookup_vrs"] = {}

# For batching
all_default_attrs["batch"] = None

computer_specific_attr_names = ["mfolder"]

# log_stream = open("logfile_run_process.log", "w")

# Configure logging
logging.basicConfig(level=logging.DEBUG, filename = "logfile.log", filemode = "w", format = "%(asctime)s - %(levelname)s - %(message)s")

do_run = False


class Process(PipelineObject):

    prefix = "PR"
    # __slots__ = tuple(all_default_attrs.keys())

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
                 lookup_vrs: dict = all_default_attrs["lookup_vrs"],
                 batch: list = all_default_attrs["batch"],
                 **kwargs) -> None:
        if self._initialized:
            return
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
        self.lookup_vrs = lookup_vrs
        self.batch = batch
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
        self._validate_vrs(inputs, input_vr_names_in_code, action, default, is_input = True)

    def validate_output_vrs(self, outputs: dict, action: Action, default: Any) -> None:
        """Validate that the output variables are correct."""
        output_vr_names_in_code = []
        if not self.is_matlab:
            output_vr_names_in_code = get_returned_variable_names(self.method)
        self._validate_vrs(outputs, output_vr_names_in_code, action, default, is_input = False)    

    def _validate_vrs(self, vr: dict, vr_names_in_code: list, action: Action, default: Any, is_input: bool = False) -> None:
        """Validate that the input and output variables are correct. They should follow the same format.
        The format is a dictionary with the variable name as the key and the variable ID as the value."""

        def _validate_dataobject_level_attr(level_attr: dict, action: Action, default_attr_names: list) -> None:
            """Validate the data object level attribute. Correct format is a dictionary with the level as the key and the attribute name as the value."""
            if not isinstance(level_attr, dict):
                raise ValueError("Data object level & attribute must be a dict!")
            for key, value in level_attr.items():
                if not isinstance(key, type):
                    raise ValueError("Data object level must be a type!")
                if value not in default_attr_names:
                    raise ValueError("Data object attribute must be a valid attribute name!")
                
        if vr == default:
            return
        self.validate_method(self.method, action, None)
        if not isinstance(vr, dict):
            raise ValueError("Variables must be a dictionary.")
        default_attr_names = DefaultAttrs(self).default_attrs.keys()
        for key, value in vr.items():
            if not isinstance(key, str):
                raise ValueError("Variable names in code must be strings.")
            if not str(key).isidentifier():
                raise ValueError("Variable names in code must be valid variable names.")
            if not isinstance(value, Variable):
                if not is_input:
                    raise ValueError("Variable ID's must be Variable objects.")
                _validate_dataobject_level_attr(value, action, default_attr_names)
            else:
                if not ResearchObjectHandler.object_exists(value.id, action):
                    raise ValueError("Variable ID's must reference existing Variables.")
        if not self.is_matlab and vr_names_in_code is not None and not all([vr_name in vr_names_in_code for vr_name in vr.keys()]):
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
        dataobject_subclasses = DataObject.__subclasses__()
        for name, vr_id in input_vr_ids_dict.items():
            if isinstance(vr_id, dict):
                cls_prefix = [key for key in vr_id.keys()][0]
                attr_name = [value for value in vr_id.values()][0]
                cls = [cls for cls in dataobject_subclasses if cls.prefix == cls_prefix][0]
                input_vrs_dict[name] = {cls: attr_name}
            else:
                vr = Variable(id = vr_id, action = action)
                input_vrs_dict[name] = vr
        return input_vrs_dict
    
    def to_json_input_vrs(self, input_vrs: dict, action: Action) -> str:
        """Convert a dictionary of input variables to a JSON string."""        
        tmp_dict = {}
        for key, value in input_vrs.items():
            if isinstance(value, dict):
                tmp_dict[key] = {key.prefix: value for key, value in value.items()} # DataObject level & attribute.
            else:
                tmp_dict[key] = value.id # Variables
        return json.dumps(tmp_dict)
    
    def from_json_output_vrs(self, output_vrs: str, action: Action) -> dict:
        """Convert a JSON string to a dictionary of output variables."""
        # from ResearchOS.DataObjects.data_object import DataObject
        data_subclasses = DataObject.__subclasses__()
        output_vr_ids_dict = json.loads(output_vrs)
        output_vrs_dict = {}
        for name, vr_id in output_vr_ids_dict.items():
            if isinstance(vr_id, dict):
                for key, value in vr_id.items():
                    cls = [cls for cls in data_subclasses if cls.prefix == key]
                    output_vrs_dict[name] = {cls: value}
            else:
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
        pGraph = ResearchObjectDigraph(action = action)
        # Temporarily add the new connections to the graph.
        # Make them all lists.
        all_edges = []
        tmp_vrs_source_pr = {}
        for vr_name_in_code, pr in vrs_source_pr.items():
            tmp_vrs_source_pr[vr_name_in_code] = pr
            if not isinstance(pr, list):
                tmp_vrs_source_pr[vr_name_in_code] = [pr]
            curr_var_list = tmp_vrs_source_pr[vr_name_in_code]
            for pr_elem in curr_var_list:
                for vr_name, vr in self.input_vrs.items():
                    if vr_name not in tmp_vrs_source_pr.keys():
                        continue
                    all_edges.append((pr_elem.id, self.id, vr.id))
        # all_edges = [(pr.id, self.id, vr.id) for vr in self.input_vrs.values() for pr in vrs_source_pr.values()]
        for edge in all_edges:
            pGraph.add_edge(edge[0], edge[1], edge_id = edge[2])
        for vr_name_in_code, pr in tmp_vrs_source_pr.items():
            if not isinstance(vr_name_in_code, str):
                raise ValueError("Variable names in code must be strings.")
            if not str(vr_name_in_code).isidentifier():
                raise ValueError("Variable names in code must be valid variable names.")
            if not all([isinstance(pr_elem, (Process, Logsheet)) for pr_elem in pr]):
                raise ValueError("Source process must be a Process or Logsheet object.")
            if not all([ResearchObjectHandler.object_exists(pr_elem.id, action) for pr_elem in pr]):
                raise ValueError("Source process must reference existing Process.")
            # if not (vr_name_in_code in self.input_vrs.keys() or vr_name_in_code in self.lookup_vrs.keys()):
            #     raise ValueError("Source process VR's must reference the input variables to this function. Ensure that the 'self.set_vrs_source_pr()' line is after the 'self.set_input_vrs()' line.")
        # Check that the PipelineObject Graph does not contain a cycle.
        if not nx.is_directed_acyclic_graph(pGraph):
            cycles = nx.simple_cycles(pGraph)
            for cycle in cycles:
                print('Cycle:', cycle)
                for node, next_node in zip(cycle, cycle[1:]):
                    print('Edge:', node, '-', next_node)
            raise ValueError("Source process VR's must not create a cycle in the PipelineObject Graph.")                        
            
    def from_json_vrs_source_pr(self, vrs_source_pr: str, action: Action) -> dict:
        """Convert a JSON string to a dictionary of source processes for the input variables."""
        vrs_source_pr_ids_dict = json.loads(vrs_source_pr)
        vrs_source_pr_dict = {}
        for name, pr_id in vrs_source_pr_ids_dict.items():            
            if not isinstance(pr_id, list):
                if pr_id.startswith(Process.prefix):
                    pr = Process(id = pr_id, action = action)
                else:
                    pr = Logsheet(id = pr_id, action = action)                
            else:
                pr = [Process(id = pr_id, action = action) if pr_id.startswith(Process.prefix) else Logsheet(id = pr_id, action = action) for pr_id in pr_id]
            vrs_source_pr_dict[name] = pr
        return vrs_source_pr_dict
    
    def to_json_vrs_source_pr(self, vrs_source_pr: dict, action: Action) -> str:
        """Convert a dictionary of source processes for the input variables to a JSON string."""
        json_dict = {}
        for vr_name, pr in vrs_source_pr.items():
            if not isinstance(pr, list):
                json_dict[vr_name] = pr.id
            else:
                json_dict[vr_name] = [value.id for value in pr]
        return json.dumps(json_dict)
    
    def to_json_output_vrs(self, output_vrs: dict, action: Action) -> str:
        """Convert a dictionary of output variables to a JSON string."""
        return json.dumps({key: value.id for key, value in output_vrs.items()})
    
    # lookup_vrs methods

    def validate_lookup_vrs(self, lookup_vrs: dict, action: Action, default: Any) -> None:
        """Validate that the lookup variables are correct.
        Dict keys are var names in code, values are dicts with keys as Variable objects and values as lists of strings of var names in code."""
        if lookup_vrs == default:
            return
        if not isinstance(lookup_vrs, dict):
            raise ValueError("Lookup variables must be a dictionary.")
        for key, value in lookup_vrs.items():
            if not isinstance(key, str):
                raise ValueError("Variable names in code must be strings.")
            if not str(key).isidentifier():
                raise ValueError("Variable names in code must be valid variable names.")
            if not isinstance(value, dict):
                raise ValueError("Lookup variable values must be a dictionary.")
            for k, v in value.items():
                if not isinstance(k, Variable):
                    raise ValueError("Lookup variable keys must be Variable objects.")
                if not all([isinstance(vr_name, str) for vr_name in v]):
                    raise ValueError("Lookup variable values must be lists of strings.")
                if not all([str(vr_name).isidentifier() for vr_name in v]):
                    raise ValueError("Lookup variable values must be lists of valid variable names.")
                # if not all([vr_name in get_input_variable_names(self.method) for vr_name in v]):
                #     raise ValueError("Lookup variable values must be lists of valid variable names in the method.")
            
    def from_json_lookup_vrs(self, lookup_vrs: str, action: Action) -> dict:
        """Convert a JSON string to a dictionary of lookup variables."""
        lookup_vrs_ids_dict = json.loads(lookup_vrs)
        lookup_vrs_dict = {}
        for key, value in lookup_vrs_ids_dict.items():
            lookup_vrs_dict[key] = {}
            for vr_id, vr_names in value.items():             
                vr = Variable(id = vr_id, action = action)
                lookup_vrs_dict[key][vr] = vr_names 
        return lookup_vrs_dict
    
    # batch methods

    def validate_batch(self, batch: list, action: Action, default: Any) -> None:
        """Validate that the batch is correct."""
        if batch == default:
            return
        if not isinstance(batch, list):
            raise ValueError("Batch must be a list.")
        data_subclasses = DataObject.__subclasses__()
        if not all([batch_elem in data_subclasses for batch_elem in batch]):
            raise ValueError("Batch elements must be DataObject types.")
        if len(batch) <= 1:
            return
        ds = Dataset(id = self.get_dataset_id(), action = action)
        schema_graph = nx.MultiDiGraph(ds.schema)
        schema_ordered = nx.topological_sort(schema_graph)
        max_idx = 0
        for batch_elem in batch:
            idx = schema_ordered.index(batch_elem)
            if idx < max_idx:
                raise ValueError("Batch elements must be in order of the schema, from highest to lowest.")
            max_idx = idx
            
    def from_json_batch(self, batch: str, action: Action) -> list:
        """Convert a JSON string to a list of batch elements."""
        prefix_list = json.loads(batch)
        if prefix_list is None:
            return None
        data_subclasses = DataObject.__subclasses__()
        # data_prefixes = [cls.prefix for cls in data_subclasses]
        return [cls for cls in data_subclasses if cls.prefix in prefix_list]    
    
    def to_json_batch(self, batch: list, action: Action) -> str:
        """Convert a list of batch elements to a JSON string."""
        if batch is None:
            return json.dumps(None)
        return json.dumps([cls.prefix for cls in batch])
    
    def to_json_lookup_vrs(self, lookup_vrs: dict, action: Action) -> str:
        """Convert a dictionary of lookup variables to a JSON string."""
        return json.dumps({key: {k.id: v for k, v in value.items()} for key, value in lookup_vrs.items()})
    
    def set_input_vrs(self, **kwargs) -> None:
        """Convenience function to set the input variables with named variables rather than a dict."""
        self.__setattr__("input_vrs", kwargs)

    def set_output_vrs(self, **kwargs) -> None:
        """Convenience function to set the output variables with named variables rather than a dict."""
        self.__setattr__("output_vrs", kwargs)

    def set_vrs_source_pr(self, **kwargs) -> None:
        """Convenience function to set the source process for the input variables with named variables rather than a dict."""
        self.__setattr__("vrs_source_pr", kwargs)

    def set_lookup_vrs(self, **kwargs) -> None:
        """Convenience function to set the lookup variables with named variables rather than a dict."""
        self.__setattr__("lookup_vrs", kwargs)

    def run(self, force_redo: bool = False) -> None:
        """Execute the attached method.
        kwargs are the input VR's."""
        start_msg = f"Running {self.mfunc_name} on {self.level.__name__}s."
        print(start_msg)
        action = Action(name = start_msg)  
        ds = Dataset(id = self.get_dataset_id(), action = action)   
        ds_defaults = DefaultAttrs(ds).default_attrs
        # Validate the dataset's addresses.
        addresses_valid = ds.validate_addresses(ds.addresses, action, ds_defaults["addresses"])        
        if addresses_valid is False:
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
        matlab_double_type = type(None)
        matlab_numeric_types = []
        if self.is_matlab:
            matlab_loaded = False
            if ProcessRunner.eng is None:
                try:            
                    print("Importing MATLAB.")
                    import matlab.engine
                    try:
                        print("Attempting to connect to an existing shared MATLAB session.")
                        print("To share a session run <matlab.engine.shareEngine('ResearchOS')> in MATLAB's Command Window and leave MATLAB open.")
                        ProcessRunner.eng = matlab.engine.connect_matlab(name = "ResearchOS")
                        print("Successfully connected to the shared 'ResearchOS' MATLAB session.")
                    except:
                        print("Failed to connect. Starting MATLAB.")
                        ProcessRunner.eng = matlab.engine.start_matlab()
                    matlab_loaded = True
                    matlab_double_type = matlab.double
                    matlab_numeric_types = (matlab.double, matlab.single, matlab.int8, matlab.uint8, matlab.int16, matlab.uint16, matlab.int32, matlab.uint32, matlab.int64, matlab.uint64)
                    ProcessRunner.matlab = matlab
                    ProcessRunner.matlab_numeric_types = matlab_numeric_types
                except:
                    print("Failed to import MATLAB.")
                    matlab_loaded = False           
                
        # 4. Run the method.
        # Get the subset of the data.
        subset = Subset(id = self.subset_id, action = action)
        subset_graph = subset.get_subset(action)

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
        
        pool = SQLiteConnectionPool()
        process_runner = ProcessRunner(self, action, schema_id, schema_graph, ds, subset_graph, matlab_loaded, ProcessRunner.eng, force_redo)
        process_runner.matlab_double_type = matlab_double_type
        # process_runner.matlab_numeric_types = matlab_numeric_types  

        process_runner.matlab_loaded = True
        if process_runner.matlab is None:
            process_runner.matlab_loaded = False              

        num_inputs = 0
        if process_runner.matlab_loaded and self.is_matlab:
            ProcessRunner.eng.addpath(self.mfolder) # Add the path to the MATLAB function. This is necessary for the MATLAB function to be found.
            try:
                num_inputs = ProcessRunner.eng.nargin(self.mfunc_name, nargout=1)
            except:
                raise ValueError(f"Function {self.mfunc_name} not found in {self.mfolder}.")
            
        process_runner.num_inputs = num_inputs        
            
        if ProcessRunner.dataset_object_graph is None:
            ProcessRunner.dataset_object_graph = ds.get_addresses_graph(objs = True, action = action)
        G = ProcessRunner.dataset_object_graph

        # Set the vrs_source_prs for any var that it wasn't set for.
        add_vr_names_source_prs = [key for key in self.input_vrs.keys() if key not in self.vrs_source_pr.keys()]        
        add_vrs_source_prs = [vr.id for name_in_code, vr in self.input_vrs.items() if name_in_code in add_vr_names_source_prs]
        sqlquery_raw = "SELECT vr_id, pr_id FROM data_values WHERE vr_id IN ({}) AND schema_id = ?".format(",".join(["?" for _ in add_vrs_source_prs]))
        sqlquery = sql_order_result(action, sqlquery_raw, ["vr_id"], single = True, user = True, computer = False)
        params = tuple(add_vrs_source_prs + [schema_id])
        vr_pr_ids_result = action.conn.cursor().execute(sqlquery, params).fetchall()
        vrs_source_prs = {}
        for vr_name_in_code, vr in self.input_vrs.items():
            for vr_pr_id in vr_pr_ids_result:
                if vr.id == vr_pr_id[0]:
                    # Same order as input variables.
                    vrs_source_prs[vr_name_in_code] = Process(id = vr_pr_id[1], action = action)

        default_attrs = DefaultAttrs(self).default_attrs
        self._setattrs(default_attrs, {"vrs_source_pr": vrs_source_prs}, action = action, pr_id = self.id)        

        schema_ordered = [n for n in nx.topological_sort(schema_graph)]
        batch_graph = self.get_batch_graph(self.batch, subset_graph, schema_ordered)

        # Parse the MultiDiGraph to get the batches to run.
        batches_dict_to_run = self.get_dict_of_batches(batch_graph, ds.id)

        for batch_dict in batches_dict_to_run:
            process_runner.run_batch(batch_dict, G)

        for node_id in level_node_ids_sorted:            
            process_runner.run_node(node_id, G)

        if process_runner.matlab_loaded and self.is_matlab:
            ProcessRunner.eng.rmpath(self.mfolder)
            
        for vr_name, vr in self.output_vrs.items():
            print(f"Saved VR {vr_name} (VR: {vr.id}).")

        if action.conn:
            pool.return_connection(action.conn)

    def get_dict_of_batches(self, graph: nx.MultiDiGraph, start_node: str) -> list:
        """Split the big dict of all the batches with node ID's into a list of dicts where each element will be passed into the "run_batch" function."""
        return {succ: self.get_dict_of_batches(graph, succ) for succ in graph.successors(start_node)}

    def _run_batch(self, batch: dict) -> None:
        """Run the Process on the specified batch of DataObjects."""
        # Get the values for each node of the batch and append them into a dict of dicts.

    def run_batch(self, batch_dict: dict) -> None:
        """Run the Process on the specified batch of DataObjects."""
        for batch_node_id, batch_node_ids in batch_dict:
            # The name of the higher level running this batch.
            # The dict of the lower levels to run.
            self._run_batch(batch_node_id, batch_node_ids)

    def get_batch_graph(self, batch: list, subgraph: nx.MultiDiGraph = nx.MultiDiGraph(), schema_ordered: list = []) -> dict:
        """Get the batch dictionary of DataObject names. Needs to be recursive to account for the nested dicts.
        At the lowest level, if there are multiple DataObjects, they will also be a dict, each being one key, with its value being None.
        Note that the Dataset node should always be in the subgraph."""    
        if batch is None or len(batch) == 0:            
            batch = schema_ordered[1:-1]
        
        # Use the fact that the subgraph is a NetworkX MultiDiGraph to generate the list of nodes that are in each batch.
        # I think we can rely on the fact that the NetworkX MultiDiGraph is a tree where each branch is of equal length.
        # This is because the schema is a tree, and the subgraph is a subgraph of the schema.
        batch_graph = nx.MultiDiGraph()
        start_node = [n for n in subgraph.nodes() if n.startswith(Dataset.prefix)][0]
        first_last_types = [schema_ordered[0], schema_ordered[-1]]
        # Get lowest Process depth from vrs_source_pr
        lowest_level_idx = -1
        for pr in self.vrs_source_pr.values():
            if pr is not None:
                level_idx = schema_ordered.index(pr.level)
                if level_idx > lowest_level_idx:
                    lowest_level_idx = level_idx
                    # lowest_level = pr.level
        schema_ordered = schema_ordered[0:lowest_level_idx+1] # +1 to include the lowest level idx.
        all_ignore_types = first_last_types
        removal_depths = [schema_ordered.index(b) for b in batch if b not in all_ignore_types] # Will never cause the Dataset node to be removed because of the +1
        for node in subgraph:
            depth = nx.shortest_path_length(subgraph, source = start_node, target = node) # Dataset node will never be able to remove itself from the graph.
            if depth not in removal_depths:
                continue
            predecessors = subgraph.predecessors(node) # There will always be predecessors because Dataset never reaches here.
            successors = subgraph.successors(node) # There will always be successors because the lowest level never reaches here.
            for pred in predecessors:
                for succ in successors:
                    batch_graph.add_edge(pred, succ)
        return batch_graph


        
        last_batch_elem_idx_in_schema_ordered = schema_ordered.index(batch[-1])
        if last_batch_elem_idx_in_schema_ordered < len(schema_ordered) - 1:
            append_elems = schema_ordered[last_batch_elem_idx_in_schema_ordered + 1:]
            batch.extend(append_elems)

        def add_to_dict(node, hierarchy, current_dict):
            # If we are at the end of the hierarchy, set the value to None
            if len(hierarchy) == 1:
                current_dict[node] = None
                return

            # Otherwise, get or create a sub-dictionary for this node
            if node not in current_dict:
                current_dict[node] = {}
            sub_dict = current_dict[node]

            # Process children nodes
            next_level = hierarchy[1]
            descendants = [n for n in subgraph.nodes() if n.startswith(next_level.prefix)]
            all_keys = list(set(key for d in descendants for key in d.keys())) + descendants
            # next_level_subgraph = subgraph.subgraph([[n] + ])   
            for child in next_level_subgraph.successors(node):
                if child.startswith(next_level.prefix):
                    add_to_dict(child, hierarchy[1:], sub_dict)

        # Start processing from the top-level nodes
        top_level = batch[0]
        for node in subgraph.nodes:
            if node.startswith(top_level.prefix):
                add_to_dict(node, batch, batch_dict)
