import os

import toml

class TOMLHandler():

    def __init__(self, file_path: str) -> None:
        """Initialize the TOMLHandler class."""
        self.file_path = file_path
        self.load_toml(file_path)

    def load_toml(self, file_path: str) -> None:
        """Load all of the attributes from the TOML file."""
        with open(file_path, "r") as f:
            toml_dict = toml.load(f)
        self.toml_dict = toml_dict

    def make_abs_path(self, path: str) -> None:
        """Make the path an absolute path."""
        root = self.toml_dict["tool"]["researchos"]["paths"]["root"]["root"]
        if not os.path.isabs(path):
            path = os.path.join(root, path)
        path = path.replace("/", os.sep)
        return path
    
    def get(self, search_str: str):
        """Get the value of the search string."""
        search_str = search_str.split(".")
        value = self.toml_dict
        for key in search_str:            
            key = str(key).lower()
            if key not in value:
                raise ValueError(f"Could not find {key} in {value} in TOML file {self.file_path}")
            value = value[key]
        return value    
    
    def file_path_to_module(self, path: str):
        """Convert a file path to a module path."""
        path = path.replace(os.sep, "/")
        path = path.replace(".py", "")
        path = path.replace("/", ".")
        return path