### Contains all steps necessary to run the `compile` CLI command.
import os
import uuid

import networkx as nx

from ResearchOS.create_dag_from_toml import create_package_dag, discover_packages, get_package_index_dict, get_runnables_in_package, get_package_bridges, bridge_packages, standardize_package_runnables_dict
from ResearchOS.run import run
from ResearchOS.furcate import get_nodes_to_furcate, polyfurcate
from ResearchOS.constants import PROCESS_NAME, PLOT_NAME, STATS_NAME, LOGSHEET_NAME, DATASET_SCHEMA_KEY, BRIDGES_KEY
from ResearchOS.helper_functions import parse_variable_name
from ResearchOS.custom_classes import Logsheet, OutputVariable
from ResearchOS.validation_classes import RunnableFactory
from ResearchOS.read_logsheet import get_logsheet_dict
from ResearchOS.substitutions import substitute_levels_subsets


def get_package_order(dag: dict) -> list:
    # Topologically sort the nodes    
    sorted_nodes = list(nx.topological_sort(dag))
    node_names = [dag.nodes[node]['node'].name for node in sorted_nodes]

    # Get the package for each node
    info_tuple = [parse_variable_name(node_name) for node_name in node_names]
    packages = [info[0] for info in info_tuple]

    # Make the list of packages unique, preserving the order
    unique_packages = []
    for package in packages:
        if package not in unique_packages and package is not None:
            unique_packages.append(package)
    return unique_packages

def compile(project_folder: str, packages_parent_folders: list = []) -> nx.MultiDiGraph:
    """Compile all packages in the project into one DAG by reading their TOML files and creating a DAG for each package. 
    Then, connect the packages together into one cohesive DAG by reading each package's bridges file."""    
    packages_folders = discover_packages(packages_parent_folders)
    packages_folders.append(project_folder) # The project folder is considered a package folder.

    dag = nx.MultiDiGraph()
    all_packages_bridges = {}

    # Get the headers from the logsheet
    logsheet_dict = get_logsheet_dict(project_folder)
    logsheet_type = RunnableFactory.create(runnable_type=LOGSHEET_NAME)
    logsheet_dict['outputs'] = logsheet_dict['headers'] # Outputs are needed for validation.
    is_valid, err_msg = logsheet_type.validate(logsheet_dict, compilation_only=True) # Validate the logsheet.
    if not is_valid:
        raise ValueError(f"The logsheet is not valid. {err_msg}")
    logsheet_dict = logsheet_type.standardize(logsheet_dict) # Standardize the logsheet.        
    
    data_objects = os.environ[DATASET_SCHEMA_KEY]
    headers_in_toml = logsheet['headers']

    # 1. Get all of the package names.
    package_names = []
    for package_folder in packages_folders:
        package_name = os.path.split(package_folder)[-1]
        package_names.append(package_name)

    # 2. Get the index dict for each package.
    index_dict = {}
    runnables_dict = {}
    for package_folder, package_name in zip(packages_folders, package_names):
        index_dict[package_name] = get_package_index_dict(package_folder)
        package_runnables_dict = get_runnables_in_package(package_folder=package_folder, package_index_dict=index_dict[package_name], runnable_keys = [PROCESS_NAME, PLOT_NAME, STATS_NAME, LOGSHEET_NAME])
        standard_package_runnables_dict = standardize_package_runnables_dict(package_runnables_dict, package_folder)
        runnables_dict[package_name] = standard_package_runnables_dict

    package_names_str = '.'.join(package_names)
    os.environ['PACKAGE_NAMES'] = package_names_str

    # Create the DAG for each package
    all_packages_dags = {}
    for package_name, package_folder in zip(package_names, packages_folders):
        package_runnables_dict = runnables_dict[package_name]
        package_index_dict = index_dict[package_name]
        all_packages_dags[package_name] = create_package_dag(package_runnables_dict, package_name=package_name)      
        all_packages_bridges[package_name] = get_package_bridges(package_folder, package_index_dict[BRIDGES_KEY])

    # Add the nodes and edges from each package to the overall DAG.
    # Right now, there are no edges between packages whatsoever.
    for package_name in package_names:
        dag.add_nodes_from(all_packages_dags[package_name].nodes(data=True))
        dag.add_edges_from(all_packages_dags[package_name].edges(data=True))    

    # Create the logsheet Runnable node.
    logsheet_attrs = {}
    logsheet_attrs['inputs'] = {}
    logsheet_attrs['outputs'] = [header for header in headers_in_toml]
    logsheet_node = Logsheet(id = str(uuid.uuid4()), name = package_name + "." + LOGSHEET_NAME, attrs = logsheet_attrs)

    # Add the logsheet node to the DAG
    mapping = {}
    for column in headers_in_toml:
        output_var = OutputVariable(id=str(uuid.uuid4()), name=package_name + "." + LOGSHEET_NAME + "." + column, attrs={})
        mapping[column] = output_var.id
        dag.add_node(output_var.id, node = output_var)
        dag.add_edge(logsheet_node.id, output_var.id)

    # Connect the packages into one cohesive DAG
    dag = bridge_packages(dag, all_packages_bridges)

    # Get the order of the packages
    packages_ordered = get_package_order(dag)

    # Get the nodes to furcate the DAG
    nodes_to_furcate = get_nodes_to_furcate(dag)

    # Substitute the levels and subsets for each package in topologically sorted order
    substitute_levels_subsets(packages_ordered, all_packages_bridges, project_folder, index_dict, dag)

    # Topologically sort the nodes to furcate by
    all_sorted_nodes = list(nx.topological_sort(dag))
    topo_sorted_nodes_to_furcate = [node for node in all_sorted_nodes if node in nodes_to_furcate]
    topo_sorted_nodes_to_furcate = list(reversed(topo_sorted_nodes_to_furcate))

    # Furcate (split) the DAG
    dag = polyfurcate(dag, topo_sorted_nodes_to_furcate)
    return dag

if __name__ == '__main__':
    project_folder = '/Users/mitchelltillman/Desktop/Work/Stevens_PhD/Non_Research_Projects/ResearchOS_Test_Project_Folder'
    packages_parent_folders = ['/Users/mitchelltillman/Documents/MATLAB/Science-Code/MATLAB/Packages']
    dag = compile(project_folder, packages_parent_folders)
    fake_project_folder = packages_parent_folders
    run(dag, project_folder=project_folder)


