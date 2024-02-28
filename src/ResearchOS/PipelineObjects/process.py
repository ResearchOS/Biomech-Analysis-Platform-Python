from typing import Any
from typing import Callable
import json, sys, os, pickle
import importlib
from hashlib import sha256
from datetime import datetime, timezone, timedelta

import networkx as nx

from ResearchOS.research_object import ResearchObject
from ResearchOS.variable import Variable
from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.PipelineObjects.subset import Subset
from ResearchOS.DataObjects.dataset import Dataset
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.code_inspector import get_returned_variable_names, get_input_variable_names
from ResearchOS.action import Action
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.default_attrs import DefaultAttrs

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

complex_attrs_list = []

try:
    import matlab.engine
    eng = matlab.engine.start_matlab()
except:
    pass


class Process(PipelineObject):

    prefix = "PR"

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
        """Validate that the Python method attribute is correct."""
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
        Returns None if the method name is not found (e.g. if code changed locations or something)"""
        method_name = json.loads(json_method)
        module_name, *attribute_path = method_name.split(".")        
        module = importlib.import_module(module_name)
        attribute = module
        for attr in attribute_path:
            attribute = getattr(attribute, attr)
        return attribute

    def to_json_method(self, method: Callable, action: Action) -> str:
        """Convert a method to a JSON string."""
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
        
    def from_json_level(self, level: str, action: Action) -> type:
        """Convert a JSON string to a Process level."""
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
        input_vrs_dict = json.loads(input_vrs)
        return {key: Variable(id = value) for key, value in input_vrs_dict.items()}
    
    def to_json_input_vrs(self, input_vrs: dict, action: Action) -> str:
        """Convert a dictionary of input variables to a JSON string."""     
        return json.dumps({key: value.id for key, value in input_vrs.items()})
    
    def from_json_output_vrs(self, output_vrs: str, action: Action) -> dict:
        """Convert a JSON string to a dictionary of output variables."""
        output_vrs_dict = json.loads(output_vrs)
        return {key: Variable(id = value) for key, value in output_vrs_dict.items()}
    
    def to_json_output_vrs(self, output_vrs: dict, action: Action) -> str:
        """Convert a dictionary of output variables to a JSON string."""
        return json.dumps({key: value.id for key, value in output_vrs.items()})
    
    def set_input_vrs(self, **kwargs) -> None:
        """Convenience function to set the input variables with named variables rather than a dict."""
        action = Action(name = "set_input_vrs")
        self.__setattr__("input_vrs", kwargs)

    def set_output_vrs(self, **kwargs) -> None:
        """Convenience function to set the output variables with named variables rather than a dict."""
        action = Action(name = "set_output_vrs")
        self.__setattr__("output_vrs", kwargs)

    def run(self) -> None:
        """Execute the attached method.
        kwargs are the input VR's."""        
        ds = Dataset(id = self.get_dataset_id())

        defaults = DefaultAttrs(self)
        default_attrs = defaults.default_attrs

        action = Action(name = f"Running {self.mfunc_name} on {self.level.__name__}s.")
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

        # 4. Run the method.
        # Get the subset of the data.
        subset_graph = Subset(id = self.subset_id).get_subset()        

        # Do the setup for MATLAB.
        if self.is_matlab:
            eng.addpath(self.mfolder, nargout=0)

        level_nodes = sorted([node for node in subset_graph if isinstance(node, self.level)], key = lambda x: x.name)
        # Iterate over each data object at this level (e.g. all ros.Trial objects in the subset.)
        schema = ds.schema
        schema_graph = nx.MultiDiGraph(schema)
        schema_order = list(nx.topological_sort(schema_graph))
        pool = SQLiteConnectionPool()
        for node in level_nodes:
            # Get the values for the input variables for this DataObject node.
            print(f"Running {self.mfunc_name} on {node.name}.")
            vr_values_in = {}
            anc_nodes = nx.ancestors(subset_graph, node)
            node_lineage = [node] + [anc_node for anc_node in anc_nodes]
            input_vrs_names_dict = {}
            for var_name_in_code, vr in self.input_vrs.items():
                vr_found = False
                input_vrs_names_dict[var_name_in_code] = vr
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
            # TODO: Make the name of this variable not hard-coded.
            if self.import_file_vr_name in vr_values_in:
                vr_values_in[self.import_file_vr_name] = data_path + self.import_file_ext

            # Get the latest (active) action_id where this DataObject was assigned a value to any of the output VR's.
            # TODO: Check vr_dataobjects table to ensure that this is an ACTIVE connection.
            cursor = action.conn.cursor()
            output_vr_ids = [vr.id for vr in self.output_vrs.values()]
            sqlquery = "SELECT action_id, dataobject_id, vr_id FROM data_values WHERE dataobject_id = ? AND vr_id IN ({})".format(",".join("?" * len(output_vr_ids)))
            params = tuple([node.id] + output_vr_ids)
            result = cursor.execute(sqlquery, params).fetchall()
            time_ordered_result = ResearchObjectHandler._get_time_ordered_result(result, action_col_num = 0)
            most_recent_result = []
            for row in time_ordered_result:
                if row not in most_recent_result:
                    most_recent_result.append(row)
            if len(most_recent_result) < len(output_vr_ids):
                output_vrs_earliest_time = datetime.min.replace(tzinfo=timezone.utc)
            else:
                sqlquery = "SELECT datetime FROM actions WHERE action_id IN ({}) ORDER BY datetime".format(",".join("?" * len(result)))
                params = tuple([r[0] for r in most_recent_result])
                result = cursor.execute(sqlquery, params).fetchall()
                output_vrs_earliest_time = datetime.strptime(result[0][0], "%Y-%m-%d %H:%M:%S.%f%z")


            # Check if the values for all the input variables are up to date. If so, skip this node.
            check_vr_values_in = {vr_name: vr_val for vr_name, vr_val in vr_values_in.items()}
            run_process = False
            input_vrs_latest_time = None
            for vr_name, vr_val in check_vr_values_in.items():
                data_blob = pickle.dumps(vr_val)
                data_blob_hash = sha256(data_blob).hexdigest()            

                # Handle hard-coded variables!
                # Check if the action ID that set this variable's hard-coded value occurs after the last time this DataObject was assigned a value to any of the output VR's
                vr = input_vrs_names_dict[vr_name]
                if vr.hard_coded_value is not None:
                    sqlquery = "SELECT action_id FROM simple_attributes WHERE object_id = ? AND attr_id = ? AND attr_value = ?"
                    attr_id = ResearchObjectHandler._get_attr_id("hard_coded_value")
                    params = (vr.id, attr_id, json.dumps(vr.hard_coded_value))
                    result = cursor.execute(sqlquery, params).fetchall()
                    if len(result) == 0:
                        run_process = True
                        break
                    sqlquery = "SELECT datetime FROM actions WHERE action_id IN ({}) ORDER BY datetime DESC LIMIT 1".format(",".join("?" * len(result)))
                    params = tuple([r for r, in result])
                    result = cursor.execute(sqlquery, params).fetchall()
                    input_vrs_latest_time = datetime.strptime(result[0][0], "%Y-%m-%d %H:%M:%S.%f%z")
                    if input_vrs_latest_time > output_vrs_earliest_time:
                        run_process = True
                        break
                else:
                    # If not None, check if it's the most recent value for this data object & vr.
                    # TODO: Fix this check, it should NOT check the current DataObject, it should check the one(s?) where the vr_id came from.
                    sqlquery = "SELECT action_id, data_blob_hash FROM data_values WHERE dataobject_id = ? AND vr_id = ? AND schema_id = ? AND data_blob_hash = ?"
                    params = (node.id, self.input_vrs[vr_name].id, schema_id, data_blob_hash)
                    result = cursor.execute(sqlquery, params).fetchall()
                    if len(result) == 0:
                        run_process = True
                        break

                    time_ordered_result = ResearchObjectHandler._get_time_ordered_result(result, action_col_num = 0)
                    latest_data_blob_hash = time_ordered_result[0][1]
                    if latest_data_blob_hash != data_blob_hash:
                        run_process = True
                        break

            if not run_process:
                print(f"Skipping {node.name} ({node.id}).")
                continue

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
            if not self.is_matlab and not all(vr in self.output_vrs for vr in output_var_names_in_code):
                raise ValueError("All of the variable names returned by this method must have been previously registered with this Process instance.")            

            # Set the output variables for this DataObject node.
            idx = -1 # For MATLAB. Requires that the args are in the proper order.
            kwargs_dict = {}
            for vr_name, vr in self.output_vrs.items():
                if not self.is_matlab:
                    idx = output_var_names_in_code.index(vr_name) # Ensure I'm pulling the right VR name because the order of the VR's coming out and the order in the output_vrs dict are probably different.
                else:
                    idx += 1
                kwargs_dict[vr_name] = vr_values_out[idx]                

            ResearchObject._setattrs(node, {}, kwargs_dict, action = action)

            # Save the output variables to the database.
            # NOTE: By doing this here, is it possible to pick up a computation from where it was left off?
            # If not, can save the output variables to the database after the entire process is done.
            action.commit = True
            action.exec = True
            action.execute(return_conn = False)

        for vr_name, vr in self.output_vrs.items():
            print(f"Saved VR {vr_name} (VR: {vr.id}).")
                
        if self.is_matlab:
            eng.rmpath(self.mfolder, nargout=0)   

        pool.return_connection(action.conn)