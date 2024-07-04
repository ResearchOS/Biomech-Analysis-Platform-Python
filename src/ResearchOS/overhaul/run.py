import os
import math

import networkx as nx

from ResearchOS.overhaul.matlab_eng import import_matlab
from ResearchOS.overhaul.create_dag_from_toml import get_package_index_dict
from ResearchOS.overhaul.constants import MAT_DATA_FOLDER_KEY, RAW_DATA_FOLDER_KEY
from ResearchOS.overhaul.data_objects import get_data_objects_in_subset
from ResearchOS.overhaul.hash_dag import get_input_variable_hashes_or_values, get_output_var_hash

def run_batch(both_dags: dict, node_attrs: dict, matlab):
    """Run an individual batch for an individual node."""
    mat_data_folder = os.environ['MAT_DATA_FOLDER']
    node = os.environ['NODE']

    # 1. Load the input variables
    input_var_metadata = get_input_variable_hashes_or_values(both_dags, node_attrs['inputs'])
    if not input_var_metadata:
        return
    output_var_metadata = []
    for output in node_attrs['outputs']:
        output_dict = {}   
        output_dict["name"] = output         
        output_dict["value"] = math.nan

        output_edge_node_name = node + "." + output
        output_dict["hash"] = get_output_var_hash(both_dags, output_edge_node_name)
        output_var_metadata.append(output_dict)
    
    # Get the file path to the mat file
    relative_path = os.environ['DATA_OBJECT'].replace('.', os.sep)
    mat_file_path = os.path.join(os.getcwd(), mat_data_folder, relative_path + '.mat')

    # 2. Execute the process for this data object. .m file also saves the data
    # Run the wrapper.m file with the input variables' metadata.
    matlab_eng = matlab['matlab_eng']
    wrapper_fcn = getattr(matlab_eng, 'wrapper')
    wrapper_fcn(input_var_metadata, output_var_metadata, mat_file_path, nargout = 0)

def run(both_dags: dict, project_folder_path: str = None):
    """Run the compiled DAG."""
    m_files_folder = 'src/ResearchOS'
    if not project_folder_path:
        project_folder_path = os.getcwd()

    # Import MATLAB   
    matlab_output = import_matlab(is_matlab=True) 
    matlab_eng = matlab_output['matlab_eng']
    matlab_eng.addpath(m_files_folder)

    # Get the MAT (i.e. processed) & raw data folder by reading the project's index.toml file
    index_dict = get_package_index_dict(project_folder_path)
    os.environ[MAT_DATA_FOLDER_KEY.upper()] = index_dict[MAT_DATA_FOLDER_KEY.lower()]
    os.environ[RAW_DATA_FOLDER_KEY.upper()] = index_dict[RAW_DATA_FOLDER_KEY.lower()]

    # Get the order of the nodes
    sorted_nodes = list(nx.topological_sort(both_dags['nodes']))

    # Run the nodes in series
    for node in sorted_nodes:
        os.environ['PACKAGE'] = both_dags['nodes'].nodes[node]['package_name']
        os.environ['NODE'] = node
        get_node_settings_and_run_batch(both_dags, node_attrs=both_dags['nodes'].nodes[node]['attributes'], matlab=matlab_output)

def get_node_settings_and_run_batch(both_dags: dict, node_attrs: dict = None, matlab = None):
    """Run an individual node"""
    # 1. Get the subset of Data Objects to operate on
    subset_name = node_attrs['subset']
    subset_of_data_objects = get_data_objects_in_subset(subset_name)    

    # Process the data objects in series
    for data_object in subset_of_data_objects:
        os.environ['DATA_OBJECT'] = data_object
        run_batch(both_dags, node_attrs, matlab = matlab)
        pass