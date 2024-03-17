from typing import Any
from typing import Callable
import json, sys, os
import importlib, logging
from copy import deepcopy

import networkx as nx

from ResearchOS.PipelineObjects.process import Process
from ResearchOS.research_object import ResearchObject
from ResearchOS.variable import Variable
from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.DataObjects.dataset import Dataset
from ResearchOS.PipelineObjects.logsheet import Logsheet
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.code_inspector import get_returned_variable_names, get_input_variable_names
from ResearchOS.action import Action
from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.stats_runner import StatsRunner
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
all_default_attrs["vrs_source_pr"] = {}
all_default_attrs["subset_ids"] = None

# For static lookup trial
all_default_attrs["lookup_vrs"] = {}

# For batching
all_default_attrs["batch"] = None

computer_specific_attr_names = ["mfolder"]

class Stats(PipelineObject):
    
    prefix = "ST"

    def __init__(self, is_matlab: bool = all_default_attrs["is_matlab"],
                 mfolder: str = all_default_attrs["mfolder"],
                 mfunc_name: str = all_default_attrs["mfunc_name"],
                 method: str = all_default_attrs["method"],
                 level: str = all_default_attrs["level"],
                 input_vrs: dict = all_default_attrs["input_vrs"],
                 vrs_source_pr: dict = all_default_attrs["vrs_source_pr"],
                 subset_ids: list = all_default_attrs["subset_ids"],
                 lookup_vrs: dict = all_default_attrs["lookup_vrs"],
                 batch: str = all_default_attrs["batch"],
                 **kwargs):
        if self._initialized:
            return
        self.is_matlab = is_matlab
        self.mfolder = mfolder
        self.mfunc_name = mfunc_name
        self.method = method
        self.level = level
        self.input_vrs = input_vrs
        self.vrs_source_pr = vrs_source_pr
        self.subset_ids = subset_ids
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
    
    ## level (Plot level) methods

    def validate_level(self, level: type, action: Action, default: Any) -> None:
        """Validate that the level is correct."""
        if level == default:
            return
        if not isinstance(level, type):
            raise ValueError("Level must be a type!")
        
    def from_json_level(self, json_level: str, action: Action) -> type:
        """Convert a JSON string to a Plot level.
        
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
        """Convert a Plot level to a JSON string."""
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
                    pr = Plot(id = pr_id, action = action)
                else:
                    pr = Logsheet(id = pr_id, action = action)                
            else:
                pr = [Plot(id = pr_id, action = action) if pr_id.startswith(Plot.prefix) else Logsheet(id = pr_id, action = action) for pr_id in pr_id]
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

    def set_vrs_source_pr(self, **kwargs) -> None:
        """Convenience function to set the source process for the input variables with named variables rather than a dict."""
        self.__setattr__("vrs_source_pr", kwargs)

    def set_lookup_vrs(self, **kwargs) -> None:
        """Convenience function to set the lookup variables with named variables rather than a dict."""
        self.__setattr__("lookup_vrs", kwargs)
    
    def run(self, force_redo: bool = False) -> None:
        """Execute the attached method.

        Args:
            force_redo (bool, optional): _description_. Defaults to False.
        """
        start_msg = f"Running {self.mfunc_name} on {self.level.__name__}s."
        print(start_msg)
        action = Action(name = start_msg)
        plot_runner = StatsRunner()        
        batches_dict_to_run, all_batches_graph, G, pool = plot_runner.prep_for_run(self, action, force_redo)
        curr_batch_graph = nx.MultiDiGraph()
        for batch_id, batch_value in batches_dict_to_run.items():
            if self.batch is not None:
                curr_batch_graph = nx.MultiDiGraph(all_batches_graph.subgraph([batch_id] + list(nx.descendants(all_batches_graph, batch_id))))
            plot_runner.run_batch(batch_id, batch_value, G, curr_batch_graph)

        if plot_runner.matlab_loaded and self.is_matlab:
            StatsRunner.matlab_eng.rmpath(self.mfolder)
            
        for vr_name, vr in self.output_vrs.items():
            print(f"Saved VR {vr_name} (VR: {vr.id}).")

        if action.conn:
            pool.return_connection(action.conn)