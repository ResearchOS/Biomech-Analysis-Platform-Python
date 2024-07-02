### Contains all steps necessary to run the `compile` CLI command.
import os

import tomli as tomllib
import networkx as nx

PACKAGES_PREFIX = 'ros-'

def discover_packages(packages_parent_folders: list = None) -> list:
    """Return a list of all packages in the specified folders.
    Packages are folders within the specified folders that start with `ros-`.
    `pyproject.toml` files are expected to be in the root of each package folder.
    Returned folders are relative or absolute, depending on the input."""

    if not packages_parent_folders:
        raise ValueError('No package folders specified.')
    
    packages_folders = []
    for folder in packages_parent_folders:
        folder.replace('/', os.sep)
        if not os.path.isdir(folder):
            continue  # Skip if not a directory
        for item in os.listdir(folder):
            if item.startswith(PACKAGES_PREFIX):                                
                # Get the full path for this file
                item = os.path.join(folder, item)
                packages_folders.append(item)
    return packages_folders

def get_package_index_path(package_folder_path: str) -> str:
    """Get the path (relative to the project root folder, which contains pyproject.toml) to the package index.toml file from pyproject.toml, `tool.researchos.index`.
    The default path is `index.toml` because it sits next to the `pyproject.toml` file."""
    pyproject_path = os.path.join(package_folder_path, 'pyproject.toml')
    with open(pyproject_path, 'rb') as f:
        pyproject_dict = tomllib.load(f)
    return os.path.join(package_folder_path, pyproject_dict['tool']['researchos']['index'])

def get_package_index_dict(package_folder_path: str) -> dict:
    """Get the paths for the package's processes, plots, and stats from the index.toml file.
    Dict keys are `processes`, `plots`, and `stats`. Values are lists of relative file paths (relative to package root folder)."""
    index_path = get_package_index_path(package_folder_path)
    with open(index_path, 'rb') as f:
        index_dict = tomllib.load(f)
    return index_dict

def get_runnables_in_package(package_folder: str = None, paths_from_index: list = None) -> dict:
    """Get the package's processes, given the paths to the processes.toml files from the index.toml.
    Call this function by indexing into the output of `get_package_index_dict` as the second argument.
    Valid keys are `processes`, `plots`, and `stats`."""
    if not package_folder:
        raise ValueError('No package specified.')
    if not paths_from_index:
        return []
    
    all_runnables_dict = {}
    for path in paths_from_index:
        path = os.path.join(package_folder, path)
        with open(path, 'rb') as f:
            runnables_dict = tomllib.load(f)
        all_runnables_dict.update(runnables_dict)
    return all_runnables_dict

def get_package_bridges(package_folder: str = None, paths_from_index: list = None) -> dict:
        """Load the bridges for the package from the package's bridges.toml file."""
        if not package_folder:
            raise ValueError('No package specified.')
        
        if not paths_from_index:
            return {}
        
        all_bridges_dict = {}
        for path in paths_from_index:
            path = os.path.join(package_folder, path)
            with open(path, 'rb') as f:
                bridges_dict = tomllib.load(f)
            all_bridges_dict.update(bridges_dict)
        return all_bridges_dict

def create_package_dag(package_runnables_dict: dict = None, package_name: str = "", dag: nx.MultiDiGraph = None) -> nx.MultiDiGraph:
    """Create a directed acyclic graph (DAG) of the package's runnables."""
    if not dag:
        dag = nx.MultiDiGraph()

    if not package_runnables_dict:
        return dag

    # 1. Create a node for each runnable
    # process, plot, stats
    for runnable_type, runnables in package_runnables_dict.items():
        # Add each node
        for runnable_name, runnable_dict in runnables.items():
            node_name = package_name + "." + runnable_name
            dag.add_node(node_name, type = runnable_type, attributes = runnable_dict, package_name = package_name)

    # 2. Create edges between runnables. 
    # Does this need to be implemented differently for process vs. plots vs. stats?
    for node_name in dag.nodes:
        node_attrs = dag.nodes[node_name]['attributes']
        if 'inputs' not in node_attrs:
            continue
        for var_name_in_code, source in node_attrs['inputs'].items():
            if not is_specified(source):
                continue
            # Isolate the function name from the input (the part of the value before the ".")
            runnable_node_name, output_var_name = source.split('.')
            source_node_name = package_name + "." + runnable_node_name
            # Ensure source node exists before adding the edge
            if source_node_name not in dag.nodes:
                raise ValueError(f"Node {source_node_name} not found in the DAG.")                
            dag.add_edge(source_node_name, node_name, input = var_name_in_code, output = output_var_name, bridge = None)
    return dag

def compile():
    """Compile all packages in the project."""
    packages_parent_folders = ['src/ResearchOS']
    packages_folders = discover_packages(packages_parent_folders)
    print('Packages folders: ', packages_folders)

    dag = nx.MultiDiGraph()
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
        dag = create_package_dag(package_runnables_dict, package_name=package_name, dag=dag)
        all_packages_bridges[package_name] = get_package_bridges(package_folder, index_dict['bridges'])

    # Connect the packages into one cohesive DAG
    dag = connect_packages(dag, all_packages_bridges)

    # Get the order of the packages
    packages_ordered = get_package_order(dag)

    # Substitute the levels and subsets for each package in topologically sorted order

    # Add the constants to the DAG

def get_package_order(dag: nx.MultiDiGraph = None):
    # Topologically sort the nodes
    sorted_nodes = list(nx.topological_sort(dag))

    # Get the package for each node
    packages = [dag.nodes[node]['package_name'] for node in sorted_nodes]

    # Make the list of packages unique, preserving the order
    unique_packages = []
    for package in packages:
        if package not in unique_packages:
            unique_packages.append(package)
    return unique_packages

def connect_packages(dag: nx.MultiDiGraph = None, all_packages_bridges: dict = None) -> nx.MultiDiGraph:
    """Read each package's bridges.toml file and connect the nodes in the DAG accordingly."""
    for package_name, package_bridges in all_packages_bridges.items():
        for bridge_name, bridge_dict in package_bridges.items():
            source_node_name = bridge_dict['source']
            source_package_name, source_runnable_node_name, tmp, source_output_var_name = source_node_name.split('.')
            source_node_name = source_package_name + "." + source_runnable_node_name

            target_node_names = bridge_dict['targets']
            if not isinstance(target_node_names, list):
                target_node_names = [target_node_names]
            for target_node_name in target_node_names:
                # Ensure the target node's input is "?"
                target_package_name, target_runnable_node_name, tmp, target_input_var_name = target_node_name.split('.')
                target_node_name = target_package_name + "." + target_runnable_node_name
                target_node_attrs = dag.nodes[target_node_name]['attributes']                
                if is_specified(target_node_attrs['inputs'][target_input_var_name]):
                    raise ValueError(f"Input variable {bridge_name} for {target_node_name} is already specified.")
                package_bridge_name = package_name + "." + bridge_name
                dag.add_edge(source_node_name, target_node_name, input = target_input_var_name, output = source_output_var_name, bridge = package_bridge_name)
    return dag


def is_specified(input: str) -> bool:
    # True if the variable is provided
    return input != "?"
        

if __name__ == '__main__':
    compile()


