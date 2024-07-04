import os
import json

import networkx as nx

import tomli as tomllib
from ResearchOS.overhaul.constants import PACKAGES_PREFIX, DATA_OBJECT_NAME_KEY, LOAD_FROM_FILE_KEY
from ResearchOS.overhaul.helper_functions import is_specified, is_dynamic_variable, is_special_dict
from ResearchOS.overhaul.helper_functions import parse_variable_name

def connect_packages(both_dags: dict, all_packages_bridges: dict = None) -> nx.MultiDiGraph:
    """Read each package's bridges.toml file and connect the nodes in the DAG accordingly."""
    for package_name, package_bridges in all_packages_bridges.items():
        for bridge_name, bridge_dict in package_bridges.items():
            if not bridge_dict:
                continue
            source_edge_node_name = bridge_dict['source']
            source_package_name, source_runnable_node_name, tmp, source_output_var_name = source_edge_node_name.split('.')
            source_node_name = source_package_name + "." + source_runnable_node_name
            source_edge_node_name = source_node_name + "." + source_output_var_name

            target_edge_node_names = bridge_dict['targets']
            if not isinstance(target_edge_node_names, list):
                target_edge_node_names = [target_edge_node_names]
            for target_edge_node_name in target_edge_node_names:
                # Ensure the target node's input is "?"
                target_package_name, target_runnable_node_name, tmp, target_input_var_name = target_edge_node_name.split('.')
                target_node_name = target_package_name + "." + target_runnable_node_name
                target_edge_node_name = target_node_name + "." + target_input_var_name
                target_node_attrs = both_dags['nodes'].nodes[target_node_name]['attributes']                
                if is_specified(target_node_attrs['inputs'][target_input_var_name]):
                    raise ValueError(f"Input variable {bridge_name} for {target_node_name} is already specified.")
                both_dags['nodes'].add_edge(source_node_name, target_node_name, bridge = package_name + "." + bridge_name)
                both_dags['edges'].add_edge(source_edge_node_name, target_edge_node_name, bridge = package_name + "." + bridge_name)
    return both_dags

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
    for key in index_dict:
        if not isinstance(index_dict[key], list):
            index_dict[key] = index_dict[key].replace('/', os.sep)
        else:
            index_dict[key] = [path.replace('/', os.sep) for path in index_dict[key]]
    # Validate the keys in the index_dict
    # all packages
    if 'processes' not in index_dict:
        raise ValueError(f"Processes not found in {index_path}.")
    # if 'plots' not in index_dict:
    #     raise ValueError(f"Plots not found in {index_path}.")
    # if 'stats' not in index_dict:
    #     raise ValueError(f"Stats not found in {index_path}.")    
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
        for runnable in runnables_dict:
            # Validate each runnables_dict!
            curr_dict = runnables_dict[runnable]
            if "level" not in curr_dict:
                curr_dict["level"] = "Trial"
            if "batch" not in curr_dict:
                curr_dict["batch"] = curr_dict["level"]
            if "path" not in curr_dict:
                raise ValueError(f"Path not found in {path}.")
            if "subset" not in curr_dict:
                raise ValueError(f"Subset not found in {path}.")
            if "inputs" not in curr_dict:
                raise ValueError(f"Inputs not found in {path}.")
            if "outputs" not in curr_dict:
                raise ValueError(f"Outputs not found in {path}.")
            curr_dict["path"] = curr_dict["path"].replace('/', os.sep)
            runnables_dict[runnable] = curr_dict
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
            if not os.path.isfile(path):
                continue
            with open(path, 'rb') as f:
                bridges_dict = tomllib.load(f)
            all_bridges_dict.update(bridges_dict)
        return all_bridges_dict

def get_nodes_dag(both_dags: dict) -> nx.MultiDiGraph:
    return both_dags['nodes']

def get_edges_dag(both_dags: dict) -> nx.MultiDiGraph:    
    return both_dags['edges']

def create_package_dag(package_runnables_dict: dict, package_name: str = "") -> nx.MultiDiGraph:
    """Create a directed acyclic graph (DAG) of the package's runnables.
    node name format: `package_name.runnable_name`
    edge format: `package_name1.runnable_name1.var1 -> package_name2.runnable_name2.var2`"""
    
    # Create a new DAG for the package
    package_dag = {}
    package_dag['nodes'] = nx.MultiDiGraph()
    package_dag['edges'] = nx.MultiDiGraph()

    # 1. Create a node for each runnable
    # process, plot, stats
    for runnable_type, runnables in package_runnables_dict.items():
        # Add each node
        for runnable_name, runnable_dict in runnables.items():
            node_name = package_name + "." + runnable_name
            package_dag['nodes'].add_node(node_name, type = runnable_type, attributes = runnable_dict, package_name = package_name)
            for input_var_name in runnable_dict['inputs']:
                for output_var_name in runnable_dict['outputs']:
                    package_dag['edges'].add_edge(node_name + "." + input_var_name, node_name + "." + output_var_name)

    # 2. Create edges between runnables. 
    # Does this need to be implemented differently for process vs. plots vs. stats?
    nodes_in_dag = list(package_dag['nodes'].nodes)
    for target_node_name in nodes_in_dag:
        node_attrs = package_dag['nodes'].nodes[target_node_name]['attributes']
        if 'inputs' not in node_attrs:
            continue
        for var_name_in_code, source in node_attrs['inputs'].items():
            if not is_specified(source):
                continue

            target_edge_node_name = target_node_name + "." + var_name_in_code

            # Isolate the function name from the input (the part of the value before the ".")
            if is_dynamic_variable(source):                
                tmp, runnable_node_name, output_var_name = parse_variable_name(source)
                source_node_name = package_name + "." + runnable_node_name
                source_edge_node_name = source_node_name + "." + output_var_name            
                # Ensure source node exists before adding the edge
                if source_node_name not in package_dag['nodes'].nodes:
                    raise ValueError(f"Node {source_node_name} not found in the DAG.")                
            elif is_special_dict(source):
                key = list(source.keys())[0]
                # value = parse_special_dict(source)
                source_node_name = package_name + "." + key
                source_edge_node_name = source_node_name + "." + var_name_in_code
            else:
                # Hard-coded constant
                source_node_name = package_name + ".constants"
                source_edge_node_name = source_node_name + "." + var_name_in_code
            package_dag['nodes'].add_edge(source_node_name, target_node_name, bridge_name = None)
            package_dag['edges'].add_edge(source_edge_node_name, target_edge_node_name, bridge_name = None)            
    return package_dag

def parse_special_dict(var_dict: dict) -> str:
    """Parse the special dictionary to get the value of the variable."""
    node = os.environ['NODE']

    if DATA_OBJECT_NAME_KEY in var_dict:
        data_objects_list = node.split('.')
        data_object_type = var_dict[DATA_OBJECT_NAME_KEY]
        data_objects_ref_list = []
        data_object_type_index = data_objects_ref_list.index(data_object_type) # Find where the data object type is in the reference list of data object types.
        data_object_name = data_objects_list[data_object_type_index]
        return data_object_name
    
    if LOAD_FROM_FILE_KEY in var_dict:
        file_name = var_dict[LOAD_FROM_FILE_KEY]
        if file_name.endswith('.toml'):
            with open(file_name, 'rb') as f:
                value = tomllib.load(f)
        elif file_name.endswith('.json'):
            with open(file_name, 'rb') as f:
                value = json.load(f)
        return value

    raise ValueError(f"Invalid special dictionary: {var_dict}.")