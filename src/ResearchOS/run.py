import os
import math

import networkx as nx

from ResearchOS.matlab_eng import import_matlab
from ResearchOS.create_dag_from_toml import get_package_index_dict
from ResearchOS.constants import SAVE_DATA_FOLDER_KEY, RAW_DATA_FOLDER_KEY, DATASET_SCHEMA_KEY, PROJECT_FOLDER_KEY
from ResearchOS.data_objects import get_data_objects_in_subset
from ResearchOS.hash_dag import get_input_variable_hashes_or_values, get_output_var_hash
from ResearchOS.helper_functions import parse_variable_name, is_specified
from ResearchOS.custom_classes import Runnable, Variable

def run_batch(dag: nx.MultiDiGraph, runnable: Runnable, matlab):
    """Run an individual batch for an individual node."""
    # mat_data_folder = os.environ[MAT_DATA_FOLDER_KEY.upper()]
    mat_data_folder = 'mat_data'
    node = os.environ['NODE_UUID']

    # 1. Load the input variables
    for input_name, input_value in runnable.inputs.items():
        if not is_specified(input_value):
            raise ValueError(f"Input variable {input_name} is not specified. This should have been resolved by now.")
    input_var_metadata = get_input_variable_hashes_or_values(dag, runnable.inputs)
    if not input_var_metadata:
        return
    output_var_metadata = []
    for output in runnable.outputs:
        output_dict = {}   
        output_dict["name"] = output         
        output_dict["value"] = math.nan

        output_edge_node_name = node + "." + output
        output_dict["hash"] = get_output_var_hash(dag, output_edge_node_name)
        output_var_metadata.append(output_dict)
    
    # Get the file path to the mat file
    relative_path = os.environ['DATA_OBJECT'].replace('.', os.sep)
    mat_file_path = os.path.join(os.getcwd(), mat_data_folder, relative_path + '.mat')

    # 2. Execute the process for this data object. .m file also saves the data
    # Run the wrapper.m file with the input variables' metadata.
    matlab_eng = matlab['matlab_eng']
    wrapper_fcn = getattr(matlab_eng, 'wrapper')
    wrapper_fcn(input_var_metadata, output_var_metadata, mat_file_path, nargout = 0)

def run(dag: nx.MultiDiGraph, start_node_name: str = None, project_folder: str = None):
    """Run the compiled DAG."""
    m_files_folder = 'src/ResearchOS/overhaul'
    if not project_folder:
        project_folder = os.getcwd()

    os.environ[PROJECT_FOLDER_KEY] = project_folder

    # Import MATLAB   
    matlab_output = import_matlab(is_matlab=True) 
    matlab_eng = matlab_output['matlab_eng']
    matlab_eng.addpath(m_files_folder)

    # Get the MAT (i.e. processed) & raw data folder by reading the project's index.toml file
    index_dict = get_package_index_dict(project_folder)
    os.environ[MAT_DATA_FOLDER_KEY] = index_dict[MAT_DATA_FOLDER_KEY.lower()]
    os.environ[RAW_DATA_FOLDER_KEY] = index_dict[RAW_DATA_FOLDER_KEY.lower()]

    # Get where to start the DAG. If not specified, include all nodes.
    dag_to_run = dag
    if start_node_name:
        # Get the UUID of the start node
        start_node = [node for _, node in dag.nodes if node['node'].name == start_node_name and isinstance(node['node'], Runnable)]
        if not start_node:
            raise ValueError(f"Specified Runnable node {start_node_name} not found in the DAG.")
        # Get the input variable nodes for the start_node.        
        input_variable_nodes = list(dag.predecessors(start_node))
        assert all([isinstance(dag.nodes[node]['node'], Variable) for node in input_variable_nodes])
        # Get all of the downstream nodes (Runnable & Variable)
        nodes_in_dag = list(nx.descendants(dag, start_node)).append(start_node).extend(input_variable_nodes)        
        dag_to_run = dag.subgraph(nodes_in_dag)

    # Get the order of the nodes
    sorted_nodes = list(nx.topological_sort(dag_to_run))
    sorted_runnable_nodes = [node for node in sorted_nodes if isinstance(dag_to_run.nodes[node]['node'], Runnable)]

    logsheet_output_file_path = os.path.join(project_folder, 'logsheet_output.mat')
    logsheet_data = matlab_eng.load(logsheet_output_file_path, nargout=1)
    schema = logsheet_data['schema']
    schema_str = '.'.join(schema)
    os.environ[DATASET_SCHEMA_KEY] = schema_str
    all_data_objects = logsheet_data['data_objects']


    # Run the nodes in series
    for node in sorted_runnable_nodes:
        node_full_name = dag_to_run.nodes[node]['node'].name
        tmp, package_name, node_name = parse_variable_name(node_full_name)
        os.environ['PACKAGE'] = package_name
        os.environ['NODE_UUID'] = node
        os.environ['NODE_FULL_NAME'] = node_full_name
        get_node_settings_and_run_batch(dag_to_run, runnable=dag.nodes[node]['node'], matlab=matlab_output, all_data_objects=all_data_objects)

def get_node_settings_and_run_batch(dag: nx.MultiDiGraph, runnable: Runnable = None, matlab = None, all_data_objects: list = None):
    """Run an individual Runnable node."""
    # 1. Get the subset of Data Objects to operate on
    subset_name = runnable.subset
    subset_of_data_objects = get_data_objects_in_subset(subset_name, all_data_objects=all_data_objects, level=runnable.level, matlab=matlab)    

    # Process the data objects in series
    for data_object in subset_of_data_objects:
        os.environ['DATA_OBJECT'] = data_object
        run_batch(dag, runnable, matlab = matlab)        