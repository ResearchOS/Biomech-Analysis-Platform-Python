from ResearchOS.overhaul.constants import LOAD_CONSTANT_FROM_FILE_KEY, LOGSHEET_VAR_KEY, DATA_FILE_KEY

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