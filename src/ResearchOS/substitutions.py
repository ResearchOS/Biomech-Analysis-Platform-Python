import os

import networkx as nx
import tomli as tomllib

from ResearchOS.custom_classes import Runnable
from ResearchOS.constants import PACKAGE_SETTINGS_KEY

def substitute_levels_subsets(packages_ordered: list, all_packages_bridges: list, project_folder: str, index_dict: dict, dag: nx.MultiDiGraph):
    """Substitute the default levels and subsets in each package with the levels and subsets specified in the project settings.
    This should be done in topological order.
    """

    for package in packages_ordered:
        if package not in all_packages_bridges:
            continue # No bridges to other packages, which is ok!
        if len(index_dict[package][PACKAGE_SETTINGS_KEY]) == 0:
            continue # This package has no settings, which is ok! (Unless it's the project's package).
        # Get the level and subset conversions for this package.
        package_settings_path = index_dict[package][PACKAGE_SETTINGS_KEY][0].replace("/", os.sep)
        project_settings_path = os.path.join(project_folder, package_settings_path)
        with open(project_settings_path, "rb") as f:
            project_settings = tomllib.load(f)
        level_conversions = project_settings['levels'] # Dict where keys are new levels and values are old levels.
        subset_conversions = project_settings['subsets'] # Dict where keys are new subsets and values are old subsets.        
        # Get the nodes in this package.
        package_nodes = [node[0] for node in dag.nodes(data=True) if node[1]['node'].name.startswith(package + ".") and isinstance(node[1]['node'], Runnable)]  
        package_ancestor_nodes = []      
        for node in package_nodes:
            curr_ancestor_nodes = list(nx.ancestors(dag, node))
            curr_ancestor_nodes = [node for node in curr_ancestor_nodes if node not in package_nodes] # Make sure to not change the package's nodes.
            package_ancestor_nodes.extend(curr_ancestor_nodes)
        # Change subset & level to the new value.
        for node in package_ancestor_nodes:
            dag.nodes['node'].subset = subset_conversions[dag.nodes['node'].subset]
            dag.nodes['node'].level = level_conversions[dag.nodes['node'].level]
            # Replace the levels of the batch with the new levels.
            for index, level in enumerate(dag.nodes['node'].batch):
                dag.nodes['node'].batch[index] = level_conversions[level]
    return dag