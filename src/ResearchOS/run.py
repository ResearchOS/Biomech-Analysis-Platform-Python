import os
import json

import networkx as nx

from ResearchOS.matlab_eng import import_matlab, check_if_matlab
from ResearchOS.constants import MATLAB_ENG_KEY, DATA_OBJECT_KEY, DATA_OBJECT_BATCH_KEY, SAVE_DATA_FOLDER_KEY, DATASET_SCHEMA_KEY, ENVIRON_VAR_DELIM
from ResearchOS.data_objects import get_data_objects_in_subset
from ResearchOS.hash_dag import get_input_variable_hashes_or_values, get_output_variable_hashes
from ResearchOS.helper_functions import is_specified
from ResearchOS.visualize_dag import get_sorted_runnable_nodes
from ResearchOS.custom_classes import Runnable
from ResearchOS.batches import get_batches_dict

def run_batch(node_settings: dict, matlab: dict):
    """Run an individual batch for an individual node."""
    # 1. Load the input variables    
    for input_name, input_value in node_settings["inputs"].items():
        # For loop is split up so the input name can be reported in the error.
        if not is_specified(input_value):
            raise ValueError(f"Input variable {input_name} is not specified. This should have been resolved by now.")
    input_var_metadata = get_input_variable_hashes_or_values(dag, runnable.inputs)
    if not input_var_metadata:
        return
    output_var_metadata = get_output_variable_hashes()
    
    # Get the file path to the mat file
    relative_path = os.environ[DATA_OBJECT_KEY].replace('.', os.sep)
    save_data_folder = os.environ[SAVE_DATA_FOLDER_KEY]
    save_file_path = os.path.join(save_data_folder, relative_path + '.mat')

    # 2. Execute the process for this data object. .m file also saves the data
    # Run the wrapper.m file with the input variables' metadata.
    matlab_eng = matlab[MATLAB_ENG_KEY]
    wrapper_fcn = getattr(matlab_eng, 'wrapper')
    wrapper_fcn(input_var_metadata, output_var_metadata, save_file_path, nargout = 0)

def run(dag: nx.MultiDiGraph):
    """Run the compiled DAG."""
    m_files_folder = 'src/ResearchOS'

    # Get the ordered list of Runnables to run.
    sorted_runnable_nodes = get_sorted_runnable_nodes(dag)

    # Import MATLAB
    is_matlab = check_if_matlab(dag, sorted_runnable_nodes)
    matlab_output = import_matlab(is_matlab=is_matlab) 
    matlab_eng = matlab_output[MATLAB_ENG_KEY]
    matlab_eng.addpath(m_files_folder)    

    # Run the nodes in series
    for node_uuid in sorted_runnable_nodes:
        node = dag.nodes[node_uuid]['node']
        node_settings = get_node_settings(dag, node)        
        result = run_batch(node_settings, matlab=matlab_output)
        if not result:
            raise ValueError("Result is not 0")
        
def get_node_settings(runnable: Runnable = None, data_object: list = []):
    # 1. Get the subset of Data Objects to operate on
    subset_name = runnable.subset    
    batch_list = runnable.batch
    subset_of_data_objects = get_data_objects_in_subset(subset_name, level=runnable.factor) # Get the list of specific data objects included in this subset.    
    # The input list becomes the top-level keys to the nested dict (at the specified factor level)
    # The values are a nested dict of data objects within each subset data object.
    # For example, if factor="Condition", then all of the Trials in that condition would be included as sub-dicts (with values = []).
    subset_data_object_batches = get_batches_dict(subset_of_data_objects, batch_list) 
    if data_object:
        # Get the batch of the current data object only, if provided.
        schema = os.environ[DATASET_SCHEMA_KEY].split(ENVIRON_VAR_DELIM)
        data_object_index = schema.index(data_object)
        current_data_object = data_object[data_object_index]
        subset_data_object_batches = {current_data_object: subset_data_object_batches[current_data_object]}
    node_settings = {}
    node_settings["language"] = runnable.language
    node_settings["subset_name"] = runnable.subset
    node_settings["subset"] = subset_of_data_objects
    node_settings["batch_name"] = runnable.batch
    node_settings["batches"] = subset_data_object_batches
    node_settings["factor"] = runnable.factor
    return node_settings

def get_node_settings_for_hash(node_settings: dict):
    """Get the settings needed for hashing a node."""
    node_settings_for_hash = {}
    node_settings_for_hash["batch_name"] = node_settings["batch_name"]
    node_settings_for_hash["factor"] = node_settings["factor"]
    node_settings_for_hash["language"] = node_settings["language"]
    return node_settings_for_hash

def run_batch(node_settings: dict, matlab: dict = None, parallel: bool = False):
    """Run an individual Runnable node."""
    # Process the data objects in series
    for data_object, data_object_batch in node_settings["batches"].items():
        os.environ[DATA_OBJECT_KEY] = data_object
        os.environ[DATA_OBJECT_BATCH_KEY] = json.dumps(data_object_batch)
        # TODO: Support parallelization if specified
        if parallel:
            raise ValueError("Parallelization not supported yet!")
        result = run_batch(node_settings, data_object_batch, matlab = matlab)
        if not result:
            return result