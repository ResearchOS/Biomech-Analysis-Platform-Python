import os

import tomli as tomllib

def get_data_objects_in_subset(subset_name: str) -> list:
    """Get the data objects in the specified subset. Returns a list of Data Object strings using dot notation.
    e.g. `Subject.Task.Trial`"""
    return ['Subject1.Task1.Trial1']

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