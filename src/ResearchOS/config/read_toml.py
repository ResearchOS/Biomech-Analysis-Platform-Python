import os, re

import toml

from ResearchOS.config.package_path import get_package_path

PYPROJECT_TOML = "pyproject.toml"

def read_toml(toml_path: str = PYPROJECT_TOML) -> dict:
    """Read the toml file and return the dictionary.
    If no toml file is passed, read the project's pyproject.toml file."""
    with open(toml_path, "r") as f:
        return toml.load(f)
    
def get_pkg_name(toml_dict: dict) -> str:
    """Return the package name from the toml dictionary."""
    return toml_dict["project"]["name"]
    
def get_index_file_path_from_pyproject(toml_dict: dict) -> str:
    """Return the index file path from the toml dictionary."""
    return toml_dict["tool"]["researchos"]["index-file"]

def get_bridges_file_path_from_index(toml_dict: dict) -> str:
    """Return the bridges file path from the toml dictionary."""
    return toml_dict["root"]["bridges-file"]

def get_class_file_paths_from_pkg_index(index_toml_dict: dict, class_name: str) -> list:
    """Return the paths to settings files from the package index file for the class specified."""
    return index_toml_dict[class_name] if isinstance(index_toml_dict[class_name], list) else [index_toml_dict[class_name]]

def get_pkg_pyproject_toml_file_path(pkg_name: str) -> dict:
    """Return the pyproject.toml path for the installed package."""
    pkg_path = get_package_path(pkg_name)
    return os.path.join(pkg_path, PYPROJECT_TOML)

def get_deps_list(toml_dict: dict) -> list:
    """Return the list of dependencies from the package's toml dictionary.
    Dependencies for each package are stored in pyproject.toml's `[project.dependencies]` table."""
    return [d for d in toml_dict["project"]["dependencies"] if check_if_pkg_is_researchos(get_pkg_pyproject_toml_file_path(parse_dep_for_pkg_name(d)))]

def parse_dep_for_pkg_name(dep: str) -> str:
    """Return the package name from the dependency string.
    Need to strip the version number that may be in the dependency string."""
    match = re.match(r'^[^=<>]*', dep)
    if match:
        return match.group(0)
    return dep

def check_if_pkg_is_researchos(toml_path: str) -> bool:
    """Check if the package is a ResearchOS package.
    Return True if pyproject.toml has a tool.researchos section."""
    toml_dict = read_toml(toml_path)
    return "tool" in toml_dict and "researchos" in toml_dict["tool"]