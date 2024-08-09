import os
from typing import Any

import toml

from ResearchOS.constants import LOAD_CONSTANT_FROM_FILE_KEY, LOGSHEET_VAR_KEY, DATA_FILE_KEY, PACKAGE_SETTINGS_KEY
from ResearchOS.compile import get_package_index_dict

def parse_variable_name(var_name: str) -> tuple:
    """Parse the variable name into its constituent parts."""    
    names = var_name.split('.')
    if len(names) == 2:
        runnable_name, var_name = names
        package_name = None
    elif len(names) == 3:
        package_name, runnable_name, var_name = names
    elif len(names) == 4:
        package_name, runnable_name, tmp, var_name = names
    else:
        raise ValueError('Invalid output_var format.')
    return package_name, runnable_name, var_name

def is_specified(input: str) -> bool:
    # True if the variable is provided
    return input != "?"

def is_dynamic_variable(var_string: str) -> bool:
    """Check if the variable is a dynamic variable."""
    # Check if it's of the form "string.string", or "string.string.string", or "string.string.string.string"
    # Also check to make sure that the strings are not just numbers.
    if not isinstance(var_string, str):
        return False
    
    names = var_string.split('.')    
    if len(names)==1 and names[0] == "__logsheet__":
        return False
    for name in names:
        if name.isdigit():
            return False
    if len(names)==1:
        return False # Not a number, but not a dynamic variable either.
    return True

def is_special_dict(var_dict: dict) -> bool:
    """Check if the variable is a special dictionary."""
    # Check if the dictionary has a key that is a dynamic variable
    if not isinstance(var_dict, dict):
        return False
    if len(var_dict.keys()) != 1:
        return False
    key = list(var_dict.keys())[0]
    if key in [DATA_OBJECT_NAME_KEY, 
               LOAD_CONSTANT_FROM_FILE_KEY,
               LOGSHEET_VAR_KEY,
               DATA_FILE_KEY
               ]:
        return True
    return False

def get_package_setting(project_folder: str, setting_name: str, default_value: Any) -> dict:
    index_dict = get_package_index_dict(project_folder)
    if PACKAGE_SETTINGS_KEY not in index_dict:
        return default_value     
    package_settings_path = index_dict[PACKAGE_SETTINGS_KEY]
    if not package_settings_path:
        return default_value      
    if len(package_settings_path) > 1:
        raise ValueError("The package settings file path is not unique in the index.toml! Default is 'src/project_settings.toml'.")
    if not os.path.isabs(package_settings_path[0]):
        package_settings_path[0] = os.path.join(project_folder, package_settings_path[0])
    with open(package_settings_path[0], "r") as f:
        package_settings_dict = toml.load(f)
    if setting_name not in package_settings_dict:
        return default_value      
    setting_dict = package_settings_dict[setting_name]
    return setting_dict