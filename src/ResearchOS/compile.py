### Contains all steps necessary to run the `compile` CLI command.
import os, sys
import json
from typing import Any
import math
# import concurrent.futures
# from functools import partial

import tomli as tomllib
import networkx as nx

PACKAGES_PREFIX = 'ros-'
LOAD_FROM_FILE_KEY = '__file__'
DATA_OBJECT_NAME_KEY = '__data_object__'

MAT_DATA_FOLDER_KEY = 'mat_data_folder'
RAW_DATA_FOLDER_KEY = 'raw_data_folder'

def graph_to_tuple(graph):
    # Extract node data. Order of the edge tuples matters.
    edges_tuple = tuple([(src, dst) for src, dst in sorted(graph.edges(data=False))])    
    return edges_tuple

def ros_hash(obj: Any) -> str:
    """Hash the input string."""
    from hashlib import sha256
    if isinstance(obj, nx.MultiDiGraph):
        obj = graph_to_tuple(obj)
    return sha256(str(obj).encode()).hexdigest()

def is_specified(input: str) -> bool:
    # True if the variable is provided
    return input != "?"

def is_dynamic_variable(var_string: str) -> bool:
    """Check if the variable is a dynamic variable."""
    # Check if it's of the form "string.string", or "string.string.string", or "string.string.string.string"
    # Also check to make sure that the strings are not just numbers.
    names = var_string.split('.')
    for name in names:
        if name.isdigit():
            return False
    return True

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
            with open(path, 'rb') as f:
                bridges_dict = tomllib.load(f)
            all_bridges_dict.update(bridges_dict)
        return all_bridges_dict

def get_nodes_dag(both_dags: dict) -> nx.MultiDiGraph:
    return both_dags['nodes']

def get_edges_dag(both_dags: dict) -> nx.MultiDiGraph:    
    return both_dags['edges']

def create_package_dag(package_runnables_dict: dict, package_name: str = "", both_dags: dict = {}) -> nx.MultiDiGraph:
    """Create a directed acyclic graph (DAG) of the package's runnables.
    node name format: `package_name.runnable_name`
    edge format: `package_name1.runnable_name1.var1 -> package_name2.runnable_name2.var2`"""
    if not both_dags:
        raise ValueError('No DAGs provided.')

    # 1. Create a node for each runnable
    # process, plot, stats
    for runnable_type, runnables in package_runnables_dict.items():
        # Add each node
        for runnable_name, runnable_dict in runnables.items():
            node_name = package_name + "." + runnable_name
            both_dags['nodes'].add_node(node_name, type = runnable_type, attributes = runnable_dict, package_name = package_name)
            for input_var_name in runnable_dict['inputs']:
                for output_var_name in runnable_dict['outputs']:
                    both_dags['edges'].add_edge(node_name + "." + input_var_name, node_name + "." + output_var_name)

    # 2. Create edges between runnables. 
    # Does this need to be implemented differently for process vs. plots vs. stats?
    nodes_in_dag = list(both_dags['nodes'].nodes)
    for target_node_name in nodes_in_dag:
        node_attrs = both_dags['nodes'].nodes[target_node_name]['attributes']
        if 'inputs' not in node_attrs:
            continue
        for var_name_in_code, source in node_attrs['inputs'].items():
            if not is_specified(source):
                continue
            # Isolate the function name from the input (the part of the value before the ".")
            runnable_node_name, output_var_name = source.split('.')
            source_node_name = package_name + "." + runnable_node_name
            source_edge_node_name = source_node_name + "." + output_var_name            
            # Ensure source node exists before adding the edge
            if source_node_name not in both_dags['nodes'].nodes:
                raise ValueError(f"Node {source_node_name} not found in the DAG.")
            target_edge_node_name = target_node_name + "." + var_name_in_code
            both_dags['nodes'].add_edge(source_node_name, target_node_name, bridge_name = None)
            both_dags['edges'].add_edge(source_edge_node_name, target_edge_node_name, bridge_name = None)            
    return both_dags

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

def compile(packages_parent_folders: list = []) -> nx.MultiDiGraph:
    """Compile all packages in the project."""
    packages_folders = discover_packages(packages_parent_folders)
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
        both_dags = create_package_dag(package_runnables_dict, package_name=package_name, both_dags=both_dags)
        all_packages_bridges[package_name] = get_package_bridges(package_folder, index_dict['bridges'])

    # Connect the packages into one cohesive DAG
    both_dags = connect_packages(both_dags, all_packages_bridges)

    # Get the order of the packages
    packages_ordered = get_package_order(both_dags)

    # Substitute the levels and subsets for each package in topologically sorted order

    # Add the constants to the DAG

    return both_dags

def get_data_objects_in_subset(subset_name: str) -> list:
    """Get the data objects in the specified subset. Returns a list of Data Object strings using dot notation.
    e.g. `Subject.Task.Trial`"""
    return ['Subject1.Task1.Trial1']

def get_input_variable_hashes_or_values(both_dags: dict, inputs: dict) -> list:
    """Prep to load/save the input/output variables from the mat file.
    1. Get the hashes for each of the input variables.
    If the variable is a constant, then use that value.
    Returns a list of dicts with keys `name` and `hash`."""
    input_vars_info = []
    for var_name_in_code, source in inputs.items():
        input_dict = {}
        input_dict["name"] = var_name_in_code
        input_dict["hash"] = math.nan
        input_dict["value"] = math.nan
        if not is_specified(source):
            return
            # raise ValueError(f"Input variable {var_name_in_code} is not specified.")
        
        # Check if it's a dynamic variable
        if is_dynamic_variable(source):
            hash = get_output_var_hash(both_dags, source)            
            input_dict["hash"] = hash
            input_vars_info.append(input_dict)
        else:
            # Check if constant is a dict with one of the special keys
            if isinstance(source, dict):
                # Load the constant from a JSON or TOML file.
                if LOAD_FROM_FILE_KEY in source:
                    file_name = source[LOAD_FROM_FILE_KEY]
                    # Check if file is .toml or .json
                    if file_name.endswith('.toml'):
                        with open(file_name, 'rb') as f:
                            input_dict["value"] = tomllib.load(f)
                    elif file_name.endswith('.json'):
                        with open(file_name, 'rb') as f:
                            input_dict["value"] = json.load(f)
                # Get the data object name
                elif DATA_OBJECT_NAME_KEY in source:
                    continue            
            else:
                input_dict["value"] = source
            input_vars_info.append(input_dict)
    return input_vars_info

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
    
def make_all_data_objects(logsheet_toml_path: str) -> dict:
    """Create data objects in memory, reading from the logsheet file.
    Each key is one Data Object, specified with dot notation, e.g. `Subject.Task.Trial`.
    Each value is a dictionary with keys for each column."""

    # 1. Read the logsheet file.
    with open(logsheet_toml_path, 'rb') as f:
        logsheet = tomllib.load(f)    
    logsheet_path = logsheet['path']
    logsheet_path.replace('/', os.sep)

    # 2. Read the logsheet object from the logsheet.toml file.
    pass

def get_output_var_hash(both_dags: dict, output_var: str = None) -> str:
    """Hash the DAG up to the node outputting the output_var, including the var itself.
    output_var is of the form "package_name.runnable_name.var_name" OR "package_name.runnable_name.outputs.var_name".
    
    NOTE: Currently, changes to output variables that are not directly involved in generating this output_var, 
    but originate from the same node as an involved output variable will be detected as requiring changes, even though technically they should not."""
    node = os.environ['NODE']
    if not output_var:
        raise ValueError('No output_var specified.')    
    
    names = output_var.split('.')
    if len(names) == 2:
        runnable_name, var_name = names
        package_name = both_dags['nodes'].nodes[node]['package_name']
    elif len(names) == 3:
        package_name, runnable_name, var_name = names
    elif len(names) == 4:
        package_name, runnable_name, tmp, var_name = names
    else:
        raise ValueError('Invalid output_var format.')
    
    edge_node_name = package_name + "." + runnable_name + "." + var_name

    # Get the ancestors of the node
    ancestors = list(nx.ancestors(both_dags['edges'], edge_node_name))
    ancestors.append(edge_node_name)
    ancestors_dag = both_dags['edges'].subgraph(ancestors)

    if len(ancestors_dag.edges) == 0:
        raise ValueError('No ancestors found for the output_var.')

    # Hash the DAG
    return ros_hash(ancestors_dag)

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

def import_matlab(is_matlab: bool):
    # Import MATLAB
    if not is_matlab:
        return
    try:
        if "matlab" in sys.modules:
            matlab_double_types = (type(None), matlab.double,)
            matlab_numeric_types = (matlab.double, matlab.single, matlab.int8, matlab.uint8, matlab.int16, matlab.uint16, matlab.int32, matlab.uint32, matlab.int64, matlab.uint64)
        else:
            print("Importing MATLAB engine...")
            import matlab.engine
            matlab_double_types = (type(None), matlab.double,)
            matlab_numeric_types = (matlab.double, matlab.single, matlab.int8, matlab.uint8, matlab.int16, matlab.uint16, matlab.int32, matlab.uint32, matlab.int64, matlab.uint64)
            try:
                print("Attempting to connect to an existing shared MATLAB session.")                
                matlab_eng = matlab.engine.connect_matlab(name = "ResearchOS")
                print("Successfully connected to the shared 'ResearchOS' MATLAB session.")
            except:
                print("Failed to connect. Starting MATLAB.")
                print("To share a session run <matlab.engine.shareEngine('ResearchOS')> in MATLAB's Command Window and leave MATLAB open.")
                matlab_eng = matlab.engine.start_matlab()
    except:
        raise ValueError("Failed to import MATLAB engine.")
    
    matlab_output = {
        "matlab_eng": matlab_eng,
        "matlab_double_types": matlab_double_types,
        "matlab_numeric_types": matlab_numeric_types
    }
    return matlab_output

if __name__ == '__main__':
    packages_parent_folders = ['/Users/mitchelltillman/Desktop/Work/Stevens_PhD/Non_Research_Projects/ResearchOS_Test_Packages_Folder']
    both_dags = compile(packages_parent_folders)
    fake_project_folder = packages_parent_folders
    run(both_dags, project_folder_path=fake_project_folder)


