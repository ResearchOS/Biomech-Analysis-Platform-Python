import os
import math
import json

import networkx as nx

from ResearchOS.matlab_eng import import_matlab
from ResearchOS.constants import MATLAB_ENG_KEY, DATA_OBJECT_KEY, DATA_OBJECT_BATCH_KEY, NODE_UUID_KEY, SAVE_DATA_FOLDER_KEY
from ResearchOS.data_objects import get_data_objects_in_subset
from ResearchOS.hash_dag import get_input_variable_hashes_or_values, get_output_variable_hashes
from ResearchOS.helper_functions import parse_variable_name, is_specified
from ResearchOS.visualize_dag import get_sorted_runnable_nodes
from ResearchOS.custom_classes import Runnable
from ResearchOS.batches import get_batches_dict

def run_batch(dag: nx.MultiDiGraph, runnable: Runnable, matlab):
    """Run an individual batch for an individual node."""
    mat_data_folder = 'mat_data'
    node = os.environ[NODE_UUID_KEY]

    # 1. Load the input variables    
    for input_name, input_value in runnable.inputs.items():
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

    # Import MATLAB
    matlab_output = import_matlab(is_matlab=True) 
    matlab_eng = matlab_output[MATLAB_ENG_KEY]
    matlab_eng.addpath(m_files_folder)

    # Get the ordered list of Runnables to run.
    sorted_runnable_nodes = get_sorted_runnable_nodes(dag)

    # Run the nodes in series
    for node_uuid in sorted_runnable_nodes:
        node = dag.nodes[node_uuid]['node']
        node_full_name = node.name
        tmp, package_name, node_name = parse_variable_name(node_full_name)
        get_node_settings_and_run_batch(dag, runnable=node, matlab=matlab_output)

def get_node_settings_and_run_batch(dag: nx.MultiDiGraph, runnable: Runnable = None, matlab: dict = None):
    """Run an individual Runnable node."""
    # 1. Get the subset of Data Objects to operate on
    subset_name = runnable.subset    
    batch_list = runnable.batch
    factor_name = runnable.factor
    subset_of_data_objects = get_data_objects_in_subset(subset_name, level=runnable.factor, matlab=matlab) # Get the list of specific data objects included in this subset.    
    # The input list becomes the top-level keys to the nested dict (at the specified factor level)
    # The values are a nested dict of data objects within each subset data object.
    # For example, if factor="Condition", then all of the Trials in that condition would be included as sub-dicts (with values = []).
    subset_data_object_batches = get_batches_dict(subset_of_data_objects, batch_list) 

    # Process the data objects in series
    for data_object, data_object_batch in subset_data_object_batches.items():
        os.environ[DATA_OBJECT_KEY] = data_object
        os.environ[DATA_OBJECT_BATCH_KEY] = json.dumps(data_object_batch)
        run_batch(dag, runnable, matlab = matlab)