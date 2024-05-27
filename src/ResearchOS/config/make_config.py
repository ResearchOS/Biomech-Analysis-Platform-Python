import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import networkx as nx

import read_toml
import ResearchOS.config.pkg_deps_graph as pkg_deps_graph

class_names = ["logsheet", "process", "plot", "stats", "subset"]

def make_config(toml_path: str = read_toml.PYPROJECT_TOML) -> "nx.MultiDiGraph":
    """Make the config, respecting the topological order of the dependencies."""
    toml_dict = read_toml.read_toml(toml_path)
    pkg_name = read_toml.get_pkg_name(toml_dict)
    G = pkg_deps_graph.get_pkg_deps_graph(pkg_name)        

    deps_list = pkg_deps_graph.sort_topo_alpha(G) # List of packages.

    for pkg in deps_list:
        G = load_package_settings(pkg, G)

    os.environ["RESEARCHOS"]["PKG_GRAPH"] = G
    return G

def load_package_settings(pkg: str, G: "nx.MultiDiGraph"):
    """Read the package index file and load the class settings files into the config graph."""
    # Package pyproject.toml file.
    pkg_toml_path = read_toml.get_pkg_pyproject_toml_file_path(pkg)
    pkg_toml_dict = read_toml.read_toml(pkg_toml_path)

    # Package index.toml file.
    pkg_index_file_path = read_toml.get_index_file_path_from_pyproject(pkg_toml_dict)
    pkg_index_toml_dict = read_toml.read_toml(pkg_index_file_path)

    # Package bridges.toml file.
    bridges_file_path = read_toml.get_bridges_file_path_from_index(pkg_index_file_path)        
    for class_name in class_names:            
        pkg_class_file_paths = read_toml.get_class_file_paths_from_pkg_index(pkg_index_toml_dict, class_name)

        # Load from the TOML files for the class.
        for file_path in pkg_class_file_paths:
            G[pkg][class_name].update(load_class_settings(file_path, G[pkg][class_name], pkg, class_name))
        
    # Load from the bridges.TOML file for the package after all of the classes have been loaded.
    G[pkg].update(load_bridges(bridges_file_path, G[pkg]))
    return G

def load_class_settings(file_path: str, prev_dict: dict, pkg: str, class_name: str) -> dict:
    """Load the class settings (TOML file paths) from the index.toml file."""
    pkg_class_toml_dict = read_toml.read_toml(file_path)
    preexist_keys = [key for key in pkg_class_toml_dict.keys() if key in prev_dict.keys()]
    if preexist_keys:
        raise ValueError(f"Keys {preexist_keys} already exist in the config for package {pkg} and class {class_name}.")
    return pkg_class_toml_dict # Each field is a research object ID/name.

def load_bridges(bridges_file_path: str, pkg_settings_dict: dict) -> dict:
    """Load the bridges from the package's bridges.toml file."""
    custom_class_names = ["subset"] # Classes with special integration requirements from the bridges file.
    auto_class_names = [class_name for class_name in class_names if class_name not in custom_class_names]
    pkg_bridges_toml_dict = read_toml.read_toml(bridges_file_path)
    # For subsets, replace all of the subsets in the package as specified.
    subsets = pkg_bridges_toml_dict.get("subsets", None)
    pkg_settings_dict = replace_subsets(subsets, pkg_settings_dict)

    # Auto-merge the settings from the bridges file. For example, after changing inputs & outputs in the bridges file, overwrite that Process's settings.
    for class_name in auto_class_names:
        for obj_name in pkg_settings_dict[class_name].keys():
            pkg_settings_dict[class_name][obj_name].update(pkg_bridges_toml_dict.get(class_name, {}).get(obj_name, {}))

    return pkg_settings_dict # Each field is a research object ID/name.

def replace_subsets(subsets: dict, settings_dict: dict) -> dict:
    """Replace the subsets in the settings dictionary."""
    for old_subset, new_subset in subsets.items():
        for class_name in class_names:
            for obj_name in settings_dict[class_name].keys():
                current_subset_in_file = settings_dict[class_name][obj_name].get("subset", None)
                if current_subset_in_file == old_subset:
                    settings_dict[class_name]["subset"] = new_subset
    return settings_dict # Each field is a research object ID/name.