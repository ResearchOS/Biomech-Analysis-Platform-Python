import os

import networkx as nx
import tomli as tomllib

from ResearchOS.custom_classes import Runnable

def substitute_levels_subsets(packages_ordered: list, all_packages_bridges: list, project_folder: str, index_dict: dict, dag: nx.MultiDiGraph):
    """Substitute the default levels and subsets in each package with the levels and subsets specified in the project settings.
    This should be done in topological order.
    """

    for package in packages_ordered:
        if package not in all_packages_bridges:
            continue # No bridges to other packages, which is ok!
        # Get the level and subset conversions for this package.
        project_settings_path = project_folder + os.sep + index_dict[package]['project_settings'].replace("/", os.sep)
        with open(project_settings_path, "rb") as f:
            project_settings = tomllib.load(f)
        level_conversions = project_settings['levels'] # Dict where keys are new levels and values are old levels.
        subset_conversions = project_settings['subsets'] # Dict where keys are new subsets and values are old subsets.
        batch_conversions = project_settings['batch'] # Dict where keys are new batches and values are old batches.
        # Get the nodes in this package.
        package_nodes = [node for node in dag.nodes if node['node'].name.startswith(package + ".") and isinstance(node['node'], Runnable)]  
        package_ancestor_nodes = []      
        for node in package_nodes:
            curr_ancestor_nodes = list(nx.ancestors(dag, node))
            curr_ancestor_nodes = [node for node in curr_ancestor_nodes if node not in package_nodes] # Make sure to not change the package's nodes.
            package_ancestor_nodes.extend(curr_ancestor_nodes)
        # Change subset & level to the new value.
        for node in package_ancestor_nodes:
            dag.nodes['node'].subset = subset_conversions[dag.nodes['node'].subset]
            dag.nodes['node'].level = level_conversions[dag.nodes['node'].level]
            dag.nodes['node'].batch = batch_conversions[dag.nodes['node'].batch]