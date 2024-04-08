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
        return path