### Contains all steps necessary to run the `compile` CLI command.
import os

import networkx as nx

from ResearchOS.overhaul.create_dag_from_toml import create_package_dag, discover_packages, get_package_index_dict, get_runnables_in_package, get_package_bridges, bridge_packages
from ResearchOS.overhaul.run import run
from ResearchOS.overhaul.furcate import get_nodes_to_furcate, polyfurcate
from ResearchOS.overhaul.constants import PROCESS_NAME, PLOT_NAME, STATS_NAME
from ResearchOS.overhaul.helper_functions import parse_variable_name


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
    """Compile all packages in the project."""    
    packages_folders = discover_packages(packages_parent_folders)
    packages_folders.append(project_folder)

    dag = nx.MultiDiGraph()
    all_packages_bridges = {}

    # Read all of the packages' TOML files.
    package_names = []
    index_dict = {}
    runnables_dict = {}
    for package_folder in packages_folders:
        package_name = os.path.split(package_folder)[-1]
        package_names.append(package_name)
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

    # Connect the packages into one cohesive DAG
    dag = bridge_packages(dag, all_packages_bridges)

    # Get the order of the packages
    packages_ordered = get_package_order(dag)

    # Get the nodes to furcate the DAG
    nodes_to_furcate = get_nodes_to_furcate(dag)

    # Substitute the levels and subsets for each package in topologically sorted order

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


