### Contains all steps necessary to run the `compile` CLI command.
import os
import uuid

import networkx as nx
import tomli as tomllib

from ResearchOS.overhaul.create_dag_from_toml import create_package_dag, discover_packages, get_package_index_dict, get_runnables_in_package, get_package_bridges, bridge_packages
from ResearchOS.overhaul.run import run
from ResearchOS.overhaul.furcate import get_nodes_to_furcate, polyfurcate
from ResearchOS.overhaul.constants import PROCESS_NAME, PLOT_NAME, STATS_NAME, LOGSHEET_NAME, DATASET_SCHEMA_KEY
from ResearchOS.overhaul.helper_functions import parse_variable_name
from ResearchOS.overhaul.custom_classes import Logsheet, OutputVariable, Runnable
from ResearchOS.overhaul.read_logsheet import get_logsheet_dict


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
    """Compile all packages in the project into one DAG."""    
    packages_folders = discover_packages(packages_parent_folders)
    packages_folders.append(project_folder) # The project folder is considered a package folder.

    dag = nx.MultiDiGraph()
    all_packages_bridges = {}

    ## Read all of the packages' TOML files.
    # 1. Get all of the package names.
    package_names = []
    for package_folder in packages_folders:
        package_name = os.path.split(package_folder)[-1]
        package_names.append(package_name)

    # 2. Get the index dict for each package.
    index_dict = {}
    runnables_dict = {}
    for package_folder, package_name in zip(packages_folders, package_names):
        index_dict[package_name] = {}
        index_dict[package_name] = get_package_index_dict(package_folder)
        processes_dict = get_runnables_in_package(package_folder=package_folder, paths_from_index=index_dict[package_name][PROCESS_NAME])
        runnables_dict[package_name] = {}
        runnables_dict[package_name][PROCESS_NAME] = processes_dict

    package_names_str = '.'.join(package_names)
    os.environ['PACKAGE_NAMES'] = package_names_str

    # Create the DAG for each package
    all_packages_dags = {}
    for package_name, package_folder in zip(package_names, packages_folders):
        package_runnables_dict = runnables_dict[package_name]
        package_index_dict = index_dict[package_name]
        all_packages_dags[package_name] = create_package_dag(package_runnables_dict, package_name=package_name)      
        all_packages_bridges[package_name] = get_package_bridges(package_folder, package_index_dict['bridges'])

    # Add the nodes and edges from each package to the overall DAG
    for package_name in package_names:
        dag.add_nodes_from(all_packages_dags[package_name].nodes(data=True))
        dag.add_edges_from(all_packages_dags[package_name].edges(data=True))

    # Get the headers from the logsheet
    logsheet = get_logsheet_dict(project_folder)
    data_objects = os.environ[DATASET_SCHEMA_KEY]
    headers_in_toml = logsheet['headers']

    # Create the logsheet Runnable node.
    logsheet_attrs = {}
    logsheet_attrs['outputs'] = [header for header in headers_in_toml]
    logsheet_node = Logsheet(id = str(uuid.uuid4()), name = LOGSHEET_NAME, attrs = logsheet_attrs)

    # Add the logsheet node to the DAG
    mapping = {}
    for column in headers_in_toml:
        output_var = OutputVariable(id=str(uuid.uuid4()), name=LOGSHEET_NAME + "." + column, attrs={})
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
    for package in packages_ordered:
        if package not in all_packages_bridges:
            continue # No bridges to other packages, which is ok!
        # Get the level and subset conversions for this package.
        project_settings_path = project_folder + os.sep + index_dict[package]['project_settings'].replace("/", os.sep)
        with open(project_settings_path, "rb") as f:
            project_settings = tomllib.load(f)
        level_conversions = project_settings['levels'] # Dict where keys are new levels and values are old levels.
        subset_conversions = project_settings['subsets'] # Dict where keys are new subsets and values are old subsets.
        # Get the nodes in this package.
        package_nodes = [node for node in dag.nodes if node['node'].name.startswith(package + ".") and isinstance(node['node'], Runnable)]  
        package_ancestor_nodes = []      
        for node in package_nodes:
            curr_ancestor_nodes = list(nx.ancestors(dag, node))
            curr_ancestor_nodes = [node for node in curr_ancestor_nodes if node not in package_nodes] # Make sure to not change the package's nodes.
            package_ancestor_nodes.extend(curr_ancestor_nodes)
        # Change subset & level to the new value.
        for node in package_ancestor_nodes:
            dag.nodes['node'].subset = subset_conversions[dag.nodes['node'].subset]
            dag.nodes['node'].level = level_conversions[dag.nodes['node'].level]

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


