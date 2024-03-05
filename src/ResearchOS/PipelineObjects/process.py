from typing import Any
from typing import Callable
import json, sys, os
import pickle
import importlib
from hashlib import sha256
from datetime import datetime, timezone
import logging, time
import gc

import networkx as nx
# from memory_profiler import profile

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

from inspect_locals import inspect_locals

all_default_attrs = {}
all_default_attrs["is_matlab"] = False
all_default_attrs["mfolder"] = None
all_default_attrs["mfunc_name"] = None

all_default_attrs["method"] = None
all_default_attrs["level"] = None
all_default_attrs["input_vrs"] = {}
all_default_attrs["output_vrs"] = {}
all_default_attrs["subset_id"] = None

all_default_attrs["import_file_ext"] = None
all_default_attrs["import_file_vr_name"] = None

computer_specific_attr_names = ["mfolder"]

log_stream = open("logfile_run_process.log", "w")

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
    
    def to_json_output_vrs(self, output_vrs: dict, action: Action) -> str:
        """Convert a dictionary of output variables to a JSON string."""
        return json.dumps({key: value.id for key, value in output_vrs.items()})
    
    def set_input_vrs(self, **kwargs) -> None:
        """Convenience function to set the input variables with named variables rather than a dict."""
        self.__setattr__("input_vrs", kwargs)

    def set_output_vrs(self, **kwargs) -> None:
        """Convenience function to set the output variables with named variables rather than a dict."""
        self.__setattr__("output_vrs", kwargs)

    def run(self) -> None:
        """Execute the attached method.
        kwargs are the input VR's."""      
        action = Action(name = f"Running {self.mfunc_name} on {self.level.__name__}s.")  
        ds = Dataset(id = self.get_dataset_id(), action = action)

        defaults = DefaultAttrs(self)
        default_attrs = defaults.default_attrs
        
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

        # Do the setup for MATLAB.
        if self.is_matlab and matlab_loaded:
            eng.addpath(self.mfolder, nargout=0)

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
        
        pool = SQLiteConnectionPool()
        for node_id in level_node_ids_sorted[:60]:
            self.run_node(node_id, schema_id, schema_order, action, self.level, ds, subset_graph, matlab_loaded, eng)
            # gc.collect()

            inspect_locals(locals(), do_run)
            
        for vr_name, vr in self.output_vrs.items():
            print(f"Saved VR {vr_name} (VR: {vr.id}).")
                
        if self.is_matlab:
            eng.rmpath(self.mfolder, nargout=0)   

        pool.return_connection(action.conn)

    # @profile(stream=log_stream)
    def run_node(self, node_id: str, schema_id: str, schema_order: list, action: Action, pr_level: type, ds: Dataset, subset_graph: nx.MultiDiGraph, matlab_loaded: bool, eng) -> None:
        load_node_msg = f"Loading {pr_level.__name__} {node_id}."
        load_node_time = time.time()    
        logging.info(load_node_msg)    
        node = pr_level(id = node_id, action = action)
        logging.debug(f"Loading took {time.time() - load_node_time} seconds.")
        start_node_msg = f"Running {self.mfunc_name} on {node.name} ({node.id})."
        start_node_time = time.time()
        logging.info(start_node_msg)
        print(start_node_msg)
        # time.sleep(0.1) # Give breathing room for MATLAB?

        inspect_locals(locals(), do_run)
        start_input_vrs_time = time.time()
        logging.info(f"Loading input VR's for {node.name} ({node.id}).")
        # Get the values for the input variables for this DataObject node. 
        # Get the lineage so I can get the file path for import functions and create the lineage.        
        anc_node_ids = nx.ancestors(subset_graph, node_id)
        anc_node_ids_list = [node for node in nx.topological_sort(subset_graph.subgraph(anc_node_ids))][::-1]
        anc_node_ids_list.remove(ds.id)
        anc_nodes = []
        # Skip the lowest level (the Process level) and the highest level (dataset)
        for idx, level in enumerate(schema_order[1:-1]):
            anc_nodes.append(level(id = anc_node_ids_list[idx], action = action))          
        vr_values_in = {}        
        node_lineage = [node] + anc_nodes + [ds] # Smallest to largest.        
        input_vrs_names_dict = {}
        for var_name_in_code, vr in self.input_vrs.items():
            vr_found = False
            input_vrs_names_dict[var_name_in_code] = vr
            if var_name_in_code == self.import_file_vr_name:
                continue # Skip the import file variable.
            if vr.hard_coded_value is not None: 
                vr_values_in[var_name_in_code] = vr.hard_coded_value
                vr_found = True
                continue
            else:
                if var_name_in_code == "fpsUsed":
                    vr.level = ResearchObjectHandler._prefix_to_class("SJ")
                else:
                    vr.level = node.__class__
                curr_node = [tmp_node for tmp_node in node_lineage if isinstance(tmp_node, vr.level)][0]
                value = curr_node.load_vr_value(vr, action)
                vr_values_in[var_name_in_code] = value
                vr_found = True
            if not vr_found:
                raise ValueError(f"Variable {vr.name} ({vr.id}) not found in __dict__ of {node}.")
        inspect_locals(locals(), do_run)
            
        done_input_vrs_time = time.time()
        logging.debug(f"Loading input VR's for {node.name} ({node.id}) took {done_input_vrs_time - start_input_vrs_time} seconds.")
                    
        data_path = ds.dataset_path        
        for node in node_lineage[1::-1]:
            data_path = os.path.join(data_path, node.name)        
        # TODO: Make the name of this variable not hard-coded.
        if self.import_file_vr_name is not None:
            file_path = data_path + self.import_file_ext
            rel_path = os.path.relpath(file_path, ds.dataset_path)
            if not os.path.exists(file_path):
                print(f"File {rel_path} does not exist. Skipping {node.name} ({node.id}).")
                return
            vr_values_in[self.import_file_vr_name] = file_path
        get_file_path_time = time.time()
        logging.debug(f"Getting file path for {node.name} ({node.id}) took {get_file_path_time - done_input_vrs_time} seconds.")

        # Get the latest action_id where this DataObject was assigned a value to any of the output VR's AND the connection between each output VR and this DataObject is active.
        # If the connection between each output VR and this DataObject is not active, then run the process.
        cursor = action.conn.cursor()
        output_vr_ids = [vr.id for vr in self.output_vrs.values()]
        run_process = False

        # Check that all output VR connections are active.
        sqlquery_raw = "SELECT is_active FROM vr_dataobjects WHERE dataobject_id = ? AND vr_id IN ({})".format(",".join("?" * len(output_vr_ids)))
        sqlquery = sql_order_result(action, sqlquery_raw, ["is_active"], single = True, user = True, computer = False)
        params = ([node.id] + output_vr_ids)
        result = cursor.execute(sqlquery, params).fetchall()            
        all_vrs_active = all([row[0] for row in result if row[0] == 1]) # Returns True if result is empty.
        if not result or not all_vrs_active:
            run_process = True
        else:
            # Get the latest action_id
            sqlquery_raw = "SELECT action_id FROM data_values WHERE dataobject_id = ? AND vr_id IN ({})".format(",".join("?" * len(output_vr_ids)))
            sqlquery = sql_order_result(action, sqlquery_raw, ["dataobject_id", "vr_id"], single = True, user = True, computer = False) # The data is ordered. The most recent action_id is the first one.
            params = tuple([node.id] + output_vr_ids)
            result = cursor.execute(sqlquery, params).fetchall() # The latest action ID
            if len(result) == 0:
                output_vrs_earliest_time = datetime.min.replace(tzinfo=timezone.utc)
            else:                
                most_recent_action_id = result[0][0] # Get the latest action_id.
                sqlquery = "SELECT datetime FROM actions WHERE action_id = ?"
                params = (most_recent_action_id,)
                result = cursor.execute(sqlquery, params).fetchall()
                output_vrs_earliest_time = datetime.strptime(result[0][0], "%Y-%m-%d %H:%M:%S.%f%z")


            # Check if the values for all the input variables are up to date. If so, skip this node.
            check_vr_values_in = {vr_name: vr_val for vr_name, vr_val in vr_values_in.items()}            
            input_vrs_latest_time = None
            for vr_name, vr_val in check_vr_values_in.items():
                start_time = time.time()
                data_blob = pickle.dumps(vr_val)
                end_time = time.time()
                execute_time = end_time - start_time
                logging.debug(f"Pickling name: {vr_name}, type: {type(vr_val)}, size: {sys.getsizeof(data_blob)} took {execute_time} seconds.")
                data_blob_hash = sha256(data_blob).hexdigest()            

                # Handle hard-coded variables!
                # Check if the action ID that set this variable's hard-coded value occurs after the last time this DataObject was assigned a value to any of the output VR's
                vr = input_vrs_names_dict[vr_name]
                if vr.hard_coded_value is not None:
                    sqlquery_raw = "SELECT action_id FROM simple_attributes WHERE object_id = ? AND attr_id = ? AND attr_value = ?"
                    sqlquery = sql_order_result(action, sqlquery_raw, ["object_id", "attr_id", "attr_value"], single = True, user = True, computer = False)
                    attr_id = ResearchObjectHandler._get_attr_id("hard_coded_value")
                    params = (vr.id, attr_id, json.dumps(vr.hard_coded_value))
                    result = cursor.execute(sqlquery, params).fetchall()
                    if len(result) == 0:
                        run_process = True
                        break
                    sqlquery = "SELECT datetime FROM actions WHERE action_id = ?"
                    params = (result[0][0],)
                    result = cursor.execute(sqlquery, params).fetchall()
                    input_vrs_latest_time = datetime.strptime(result[0][0], "%Y-%m-%d %H:%M:%S.%f%z")
                    if input_vrs_latest_time > output_vrs_earliest_time:
                        run_process = True
                        break
                else:
                    # If not None, check if it's the most recent value for this vr, for any dataobject.
                    sqlquery_raw = "SELECT action_id, data_blob_hash FROM data_values WHERE vr_id = ? AND schema_id = ?"
                    sqlquery = sql_order_result(action, sqlquery_raw, ["vr_id", "schema_id"], single = True, user = True, computer = False)
                    params = (self.input_vrs[vr_name].id, schema_id)
                    result = cursor.execute(sqlquery, params).fetchall()
                    if len(result) == 0:
                        run_process = True
                        break

                    latest_data_blob_hash = result[0][1]
                    if latest_data_blob_hash != data_blob_hash:
                        run_process = True
                        break

        if not run_process:
            skip_msg = f"Skipping {node.name} ({node.id})."
            logging.info(skip_msg)
            print(skip_msg)
            return

        # NOTE: For now, assuming that there is only one return statement in the entire method.
        inspect_locals(locals(), do_run)
        start_run_time = time.time()
        if self.is_matlab:
            if not matlab_loaded:
                raise ValueError("MATLAB is not loaded.")
            vr_vals_in = list(vr_values_in.values())
            fcn = getattr(eng, self.mfunc_name)
            vr_values_out = fcn(*vr_vals_in, nargout=len(self.output_vrs))                               
        else:
            vr_values_out = self.method(**vr_values_in) # Ensure that the method returns a tuple.
        if not isinstance(vr_values_out, tuple):
            vr_values_out = (vr_values_out,)
        if len(vr_values_out) != len(self.output_vrs):
            raise ValueError("The number of variables returned by the method must match the number of output variables registered with this Process instance.")
        if not self.is_matlab and not all(vr in self.output_vrs for vr in output_var_names_in_code):
            raise ValueError("All of the variable names returned by this method must have been previously registered with this Process instance.")
        end_run_time = time.time()
        run_time = end_run_time - start_run_time
        run_msg = f"Running {self.mfunc_name} on {node.name} took {run_time} seconds."
        logging.debug(run_msg)

        # Set the output variables for this DataObject node.
        idx = -1 # For MATLAB. Requires that the args are in the proper order.
        kwargs_dict = {}
        for vr_name, vr in self.output_vrs.items():
            if not self.is_matlab:
                idx = output_var_names_in_code.index(vr_name) # Ensure I'm pulling the right VR name because the order of the VR's coming out and the order in the output_vrs dict are probably different.
            else:
                idx += 1
            kwargs_dict[vr] = vr_values_out[idx]   
        inspect_locals(locals(), do_run)             

        vr_values_out = []
        print(f"Size of action = ", sys.getsizeof(action.dobjs))
        node._setattrs({}, kwargs_dict, action = action)
        kwargs_dict = {}    
        inspect_locals(locals(), True)    

        end_node_before_execute_time = time.time()
        run_node_before_execute_time = end_node_before_execute_time - start_node_time
        logging.debug(f"Running {node.name} without execute took {run_node_before_execute_time} seconds.")

        # Save the output variables to the database.
        # NOTE: By doing this here, is it possible to pick up a computation from where it was left off?
        # If not, can save the output variables to the database after the entire process is done.
        action.commit = True
        action.exec = True
        action.execute(return_conn = False)

        end_node_after_execute_time = time.time()
        action_execute_time = end_node_after_execute_time - end_node_before_execute_time
        logging.debug(f"Action.execute() for {node.name} took {action_execute_time} seconds.")
        del node
        inspect_locals(locals(), do_run)