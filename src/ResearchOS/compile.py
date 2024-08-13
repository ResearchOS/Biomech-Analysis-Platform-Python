### Contains all steps necessary to run the `compile` CLI command.
import os
import uuid

import networkx as nx

from ResearchOS.create_dag_from_toml import create_package_dag, discover_packages, get_package_index_dict, get_runnables_in_package, get_package_bridges, bridge_packages, standardize_package_runnables_dict, standardize_package_bridges
from ResearchOS.run import run
from ResearchOS.furcate import get_nodes_to_furcate, polyfurcate
from ResearchOS.constants import PROCESS_NAME, PLOT_NAME, STATS_NAME, LOGSHEET_NAME, DATASET_SCHEMA_KEY, BRIDGES_KEY, DATASET_FILE_SCHEMA_KEY, ENVIRON_VAR_DELIM
from ResearchOS.constants import SAVE_DATA_FOLDER_KEY, RAW_DATA_FOLDER_KEY
from ResearchOS.helper_functions import get_package_setting
from ResearchOS.custom_classes import Logsheet, OutputVariable
from ResearchOS.read_logsheet import get_logsheet_dict
from ResearchOS.substitutions import substitute_levels_subsets
from ResearchOS.visualize_dag import visualize_compiled_dag, get_sorted_runnable_nodes

def get_package_order(dag: dict) -> list:    
    sorted_runnable_nodes = get_sorted_runnable_nodes(dag)
    node_names = [dag.nodes[node]['node'].name for node in sorted_runnable_nodes]

    # Get the package for each node
    packages = []
    for node in node_names:
        # Remove everything after the first '.'
        package_name = node.split('.')[0]
        packages.append(package_name)

    # Make the list of packages unique, preserving the order
    unique_packages = []
    for package in packages:
        if package not in unique_packages and package is not None:
            unique_packages.append(package)
    return unique_packages

def compile(project_folder: str, packages_parent_folders: list = []) -> nx.MultiDiGraph:
    """Compile all packages in the project into one DAG by reading their TOML files and creating a DAG for each package. 
    Then, connect the packages together into one cohesive DAG by reading each package's bridges file."""    
    dag, project_name, all_packages_bridges, index_dict = compile_packages_to_dag(project_folder, packages_parent_folders)

    # Get the order of the packages
    packages_ordered = get_package_order(dag)
    assert packages_ordered[0] == project_name, "The first package in the order should be the project folder."

    # Get the nodes to furcate the DAG
    nodes_to_furcate = get_nodes_to_furcate(dag)

    # Substitute the levels and subsets for each package in topologically sorted order
    dag = substitute_levels_subsets(packages_ordered, all_packages_bridges, project_folder, index_dict, dag)

    # Topologically sort the nodes to furcate by
    all_sorted_nodes = list(nx.topological_sort(dag))
    topo_sorted_nodes_to_furcate = [node for node in all_sorted_nodes if node in nodes_to_furcate]
    topo_sorted_nodes_to_furcate = list(reversed(topo_sorted_nodes_to_furcate))

    # Furcate (split) the DAG
    dag = polyfurcate(dag, topo_sorted_nodes_to_furcate)
    return dag

def compile_packages_to_dag(project_folder: str, packages_parent_folders: list = []) -> tuple:
    """Compile the DAG given the project folder. Does not perform polyfurcation."""
    if not os.path.exists(project_folder):
        raise FileNotFoundError(f"The project folder {project_folder} does not exist.") 
    if not packages_parent_folders:
        packages_parent_folders = [os.path.dirname(project_folder)]
    if project_folder not in packages_parent_folders:
        packages_parent_folders.append(project_folder)
    packages_folders = discover_packages(packages_parent_folders)
    packages_folders.append(project_folder) # The project folder is considered a package folder.

    project_name = os.path.split(project_folder)[-1].lower()
    os.environ['project_folder'] = project_folder    
    os.environ['project_name'] = project_name

    dag = nx.MultiDiGraph()
    all_packages_bridges = {}

    # Get the package settings & store them in the environment
    logsheet_dict           = get_logsheet_dict(project_folder)
    dataset_file_schema     = get_package_setting(project_folder, setting_name="dataset_file_schema", default_value=["Dataset"])
    dataset_schema          = get_package_setting(project_folder, setting_name="dataset_schema", default_value=["Dataset"])
    save_data_folder        = get_package_setting(project_folder, setting_name="mat_data_folder", default_value="saved_data")
    raw_data_folder         = get_package_setting(project_folder, setting_name="raw_data_folder", default_value="raw_data")  
    
    os.environ[DATASET_SCHEMA_KEY] = ENVIRON_VAR_DELIM.join(dataset_schema)
    os.environ[DATASET_FILE_SCHEMA_KEY] = ENVIRON_VAR_DELIM.join(dataset_file_schema)
    os.environ[SAVE_DATA_FOLDER_KEY] = project_folder + os.sep + save_data_folder if not os.path.isabs(save_data_folder) else save_data_folder
    os.environ[RAW_DATA_FOLDER_KEY] = project_folder + os.sep + raw_data_folder if not os.path.isabs(raw_data_folder) else raw_data_folder        

    # 1. Get all of the package names.
    package_names = []
    for package_folder in packages_folders:
        package_name = os.path.split(package_folder)[-1].lower()
        package_names.append(package_name)

    package_names_str = ENVIRON_VAR_DELIM.join(package_names).lower()
    os.environ['package_names'] = package_names_str

    # 2. Get the index dict for each package.
    index_dict = {}
    runnables_dict = {}
    for package_folder, package_name in zip(packages_folders, package_names):
        index_dict[package_name] = get_package_index_dict(package_folder)
        package_runnables_dict = get_runnables_in_package(package_folder=package_folder, package_index_dict=index_dict[package_name], runnable_keys = [PROCESS_NAME, PLOT_NAME, STATS_NAME, LOGSHEET_NAME])
        standard_package_runnables_dict = standardize_package_runnables_dict(package_runnables_dict, package_folder)
        runnables_dict[package_name] = standard_package_runnables_dict    

    # Create the DAG for each package
    all_packages_dags = {}
    for package_name, package_folder in zip(package_names, packages_folders):
        package_runnables_dict = runnables_dict[package_name]
        package_index_dict = index_dict[package_name]
        all_packages_dags[package_name] = create_package_dag(package_runnables_dict, package_folder=package_folder)      
        package_bridges = get_package_bridges(package_folder, package_index_dict[BRIDGES_KEY])
        all_packages_bridges[package_name] = standardize_package_bridges(package_bridges, package_folder)

    # Add the nodes and edges from each package to the overall DAG.
    # Right now, there are no edges between packages whatsoever.
    for package_name in package_names:
        dag.add_nodes_from(all_packages_dags[package_name].nodes(data=True))
        dag.add_edges_from(all_packages_dags[package_name].edges(data=True))    

    # Create the logsheet Runnable node.
    logsheet_attrs = {}
    headers_in_toml = logsheet_dict['headers']
    logsheet_attrs['outputs'] = [header for header in headers_in_toml]
    logsheet_node = Logsheet(id = str(uuid.uuid4()), name = project_name + "." + LOGSHEET_NAME, attrs = logsheet_attrs)

    # Add the logsheet node to the DAG
    mapping = {}
    dag.add_node(logsheet_node.id, node = logsheet_node)
    for column in logsheet_dict['outputs']:
        output_var = OutputVariable(id=str(uuid.uuid4()), name=package_name + "." + LOGSHEET_NAME + "." + column, attrs={})
        mapping[column] = output_var.id
        dag.add_node(output_var.id, node = output_var)
        dag.add_edge(logsheet_node.id, output_var.id)

    # Connect the packages into one cohesive DAG
    dag = bridge_packages(dag, all_packages_bridges, packages_folders)  

    return dag, project_name, all_packages_bridges, index_dict  

if __name__ == '__main__':
    project_folder = '/Users/mitchelltillman/Desktop/Work/Stevens_PhD/Non_Research_Projects/ResearchOS_Test_Project_Folder'
    packages_parent_folders = ['/Users/mitchelltillman/Documents/MATLAB/Science-Code/MATLAB/Packages']
    dag = compile(project_folder, packages_parent_folders)
    fake_project_folder = packages_parent_folders
    run(dag, project_folder=project_folder)


