### Contains all steps necessary to run the `compile` CLI command.
import os

import networkx as nx

from ResearchOS.overhaul.create_dag_from_toml import create_package_dag, discover_packages, get_package_index_dict, get_runnables_in_package, get_package_bridges, connect_packages
from ResearchOS.overhaul.run import run
from ResearchOS.overhaul.furcate import get_nodes_to_furcate


def get_package_order(both_dags: dict) -> list:
    # Topologically sort the nodes
    sorted_nodes = list(nx.topological_sort(both_dags['nodes']))

    # Get the package for each node
    packages = [both_dags['nodes'].nodes[node]['package_name'] for node in sorted_nodes]

    # Make the list of packages unique, preserving the order
    unique_packages = []
    for package in packages:
        if package not in unique_packages:
            unique_packages.append(package)
    return unique_packages

def compile(project_folder: str, packages_parent_folders: list = []) -> nx.MultiDiGraph:
    """Compile all packages in the project."""    
    packages_folders = discover_packages(packages_parent_folders)
    packages_folders.append(project_folder)
    print('Packages folders: ', packages_folders)

    both_dags = {}
    both_dags['nodes'] = nx.MultiDiGraph()
    both_dags['edges'] = nx.MultiDiGraph()
    all_packages_bridges = {}

    # Per-package operations
    for package_folder in packages_folders:
        package_name = os.path.split(package_folder)[-1]
        index_dict = get_package_index_dict(package_folder)
        print('Package ', package_folder, ' Index: ', index_dict)
        processes_dict = get_runnables_in_package(package_folder=package_folder, paths_from_index=index_dict['processes'])
        print('Package ', package_folder, ' Processes: ', processes_dict)
        package_runnables_dict = {}
        package_runnables_dict['processes'] = processes_dict        
        package_dag = create_package_dag(package_runnables_dict, package_name=package_name)
        both_dags['nodes'].add_nodes_from(package_dag['nodes'])
        if len(package_dag['nodes'].edges) > 0:
            both_dags['nodes'].add_edges_from(package_dag['nodes'].edges)
        both_dags['edges'].add_nodes_from(package_dag['edges'])
        if len(package_dag['edges'].edges) > 0:
            both_dags['edges'].add_edges_from(package_dag['edges'].edges)        
        all_packages_bridges[package_name] = get_package_bridges(package_folder, index_dict['bridges'])

    # Connect the packages into one cohesive DAG
    both_dags = connect_packages(both_dags, all_packages_bridges)

    # Get the order of the packages
    packages_ordered = get_package_order(both_dags)

    # Get the nodes to furcate the DAG
    nodes_to_furcate = get_nodes_to_furcate(both_dags)

    # Substitute the levels and subsets for each package in topologically sorted order

    # Add the constants to the DAG

    return both_dags

if __name__ == '__main__':
    project_folder = '/Users/mitchelltillman/Desktop/Work/Stevens_PhD/Non_Research_Projects/ResearchOS_Test_Project_Folder'
    packages_parent_folders = ['/Users/mitchelltillman/Documents/MATLAB/Science-Code/MATLAB/Packages']
    both_dags = compile(project_folder, packages_parent_folders)
    fake_project_folder = packages_parent_folders
    run(both_dags, project_folder_path=fake_project_folder)


