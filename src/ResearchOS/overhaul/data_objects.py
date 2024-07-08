import os

import tomli as tomllib

from ResearchOS.overhaul.constants import DATASET_SCHEMA_KEY, SUBSET_KEY
from ResearchOS.overhaul.create_dag_from_toml import get_package_index_dict

numeric_logic_options = (">", "<", ">=", "<=", )
any_type_logic_options = ("==", '=', "!=", "in", "not in", "is", "is not", "contains", "not contains")
logic_options = numeric_logic_options + any_type_logic_options
plural_logic = ("in", "not in", "contains", "not contains")

def get_data_objects_in_subset(subset_name: str, all_data_objects: list, level: str, matlab) -> list:
    """Get the data objects in the specified subset. Returns a list of Data Object strings using dot notation.
    e.g. `Subject.Task.Trial`"""
    # 1. Read the subset to determine which variables need to be loaded from file.
    # Store the variables in a dictionary.
    schema_str = os.environ[DATASET_SCHEMA_KEY]
    schema = schema_str.split(".")
    level_idx_in_schema = schema.index(level)
    # Remove everything that's not the level of interest.
    # Minus 1 on the level index to account for the Dataset level at the beginning.
    all_data_objects = [data_object for data_object in all_data_objects if str(data_object).count(".") == level_idx_in_schema - 1]

    # 2. Get the subset conditions given by the subset name.
    subset_conditions = get_subset_conditions(subset_name)
    subset_conditions_list = []
    _extract_and_replace_lists(subset_conditions, subset_conditions_list)

    # 3. Get all of the variables used in the subset's conditions.
    vars_list = []
    for condition in subset_conditions_list:
        vars_list.append(condition[0])

    all_vars = {}
    matlab_eng = matlab['matlab_eng']
    mat_data_folder = os.environ['PROJECT_FOLDER']
    for data_object in all_data_objects:
        all_vars[data_object] = {}             
        # Load the variables all at once from the file.
        mat_file_path = os.path.join(mat_data_folder, data_object.replace(".", os.sep) + ".mat")
        vars_dict = matlab_eng.readMatFileSafe(mat_file_path, vars_list)
        all_vars[data_object] = vars_dict

    # 4. For each data object, evaluate whether the subset condition is met.
    data_objects_in_subset = []
    for data_object in all_data_objects:
        if _meets_conditions(data_object, conditions=subset_conditions, vr_values = all_vars[data_object], all_data_objects=all_data_objects):
            data_objects_in_subset.append(data_object)        
    return data_objects_in_subset

def _extract_and_replace_lists(data, extracted_lists: list, counter=[0]):
    """Recursively traverses the data structure, replaces each list with a unique number, and extracts the lists. """
    if isinstance(data, list):
        # Append the current list to the extracted lists
        extracted_lists.append(data)
        # Replace the list with the current counter value
        number = counter[0]
        counter[0] += 1
        return number
    elif isinstance(data, dict):
        # Traverse dictionary and process each value
        return {key: [_extract_and_replace_lists(item, extracted_lists, counter) if isinstance(item, list) else item for item in value] if isinstance(value, list) else _extract_and_replace_lists(value, extracted_lists, counter) for key, value in data.items()}
    else:
        # For other data types, return as is
        return data

def get_subset_conditions(subset_name: str) -> dict:
    """Get the conditions for the subset."""
    folder_path = os.environ['PROJECT_FOLDER']
    index_dict = get_package_index_dict(package_folder_path=folder_path)
    subset_settings_path = index_dict[SUBSET_KEY]
    if isinstance(subset_settings_path, list):
        subset_settings_path = subset_settings_path[0]
    subset_settings_path = subset_settings_path.replace("/", os.sep)
    subset_settings_path = os.path.join(folder_path, subset_settings_path)
    with open(subset_settings_path, "rb") as f:
        subset_settings = tomllib.load(f)
    return subset_settings[subset_name]


def _meets_conditions(node_id: str, conditions: dict, vr_values: dict, all_data_objects: dict) -> bool:
    """Check if the node_id meets the conditions."""
    if isinstance(conditions, dict):
        if "and" in conditions:
            for cond in conditions["and"]:
                if not _meets_conditions(node_id, cond, vr_values, all_data_objects):
                    return False
            return True
        if "or" in conditions:
            return any([_meets_conditions(node_id, cond, vr_values, all_data_objects) for cond in conditions["or"]])
                
    # Check the condition.
    vr_id = conditions[0]
    logic = conditions[1]
    value = conditions[2]
    try:
        vr_value = vr_values[vr_id][node_id]
        found_attr = True
    except:            
        found_attr = False
        anc_nodes = node_id.split(".")
        anc_nodes.pop() # Remove the last node
        for anc_node_id in anc_nodes:
            try:
                vr_value = all_data_objects[anc_node_id][vr_id]
            except:
                continue
            found_attr = True
            break
    if not found_attr:
        return False
    
    if isinstance(vr_value, str):
        vr_value = vr_value.lower()
    if isinstance(value, str):
        value = value.lower()
    if isinstance(value, list):
        value = [x.lower() if isinstance(x, str) else x for x in value]

    # This is probably shoddy logic, but it'll serve as a first pass to handle None types.
    if logic in plural_logic:
        if logic == "contains" and vr_value is None:
            return False
        elif logic == "not contains" and vr_value is None and value is not None:                
            return True
        elif logic == "in" and value is None:
            return False
        elif logic == "not in" and value is None:
            return True
        
    if not isinstance(vr_value, str) and isinstance(value, str):
        if logic in ("contains", "not contains"):
            vr_value = [vr_value]
        elif logic in ("in", "not in"):
            value = [value]

    # Numeric
    bool_val = False
    if logic == ">" and vr_value > value:
        bool_val = True
    elif logic == "<" and vr_value < value:
        bool_val = True
    elif logic == ">=" and vr_value >= value:
        bool_val = True
    elif logic == "<=" and vr_value <= value:
        bool_val = True
    # Any type
    elif logic in ["==","="] and vr_value == value:
        bool_val = True
    elif logic == "!=" and vr_value != value:
        bool_val = True
    elif logic == "in" and vr_value in value:
        bool_val = True
    elif logic == "not in" and vr_value not in value:
        bool_val = True
    elif logic == "is" and vr_value is value:
        bool_val = True
    elif logic == "is not" and vr_value is not value:
        bool_val = True
    elif logic == "contains" and value in vr_value:
        bool_val = True
    elif logic == "not contains" and not value in vr_value:
        bool_val = True

    return bool_val