import os
import math
import json
from typing import Any

import tomli as tomllib
import networkx as nx

from ResearchOS.overhaul.constants import LOAD_CONSTANT_FROM_FILE_KEY
from ResearchOS.overhaul.helper_functions import is_specified, is_dynamic_variable

def graph_to_tuple(graph):
    # Extract node data. Order of the edge tuples matters.
    edges_tuple = tuple([(src, dst) for src, dst in sorted(graph.edges(data=False))])    
    return edges_tuple

def ros_hash(obj: Any) -> str:
    """Hash the input string."""
    from hashlib import sha224
    hash_fcn = sha224
    if isinstance(obj, nx.MultiDiGraph):
        obj = graph_to_tuple(obj)
    return 'ros_' + hash_fcn(str(obj).encode()).hexdigest()

def get_output_var_hash(dag: nx.MultiDiGraph, output_var_id: str = None) -> str:
    """Hash the DAG up to the node outputting the output_var, including the var itself.
    output_var is of the form "package_name.runnable_name.var_name" OR "package_name.runnable_name.outputs.var_name"."""
    if not output_var_id:
        raise ValueError('No output_var specified.')

    # Get the ancestors of the node
    ancestors = list(nx.ancestors(dag, output_var_id))
    ancestors.append(output_var_id)
    ancestors_dag = dag.subgraph(ancestors)

    if len(ancestors_dag.edges) == 0:
        raise ValueError('No ancestors found for the output_var.')

    # Hash the DAG
    return ros_hash(ancestors_dag)

def get_input_variable_hashes_or_values(dag: nx.MultiDiGraph, inputs: dict) -> list:
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
            hash = get_output_var_hash(dag, source)            
            input_dict["hash"] = hash
            input_vars_info.append(input_dict)
        else:
            # Check if constant is a dict with one of the special keys
            if isinstance(source, dict):
                # Load the constant from a JSON or TOML file.
                if LOAD_CONSTANT_FROM_FILE_KEY in source:
                    file_name = source[LOAD_CONSTANT_FROM_FILE_KEY]
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