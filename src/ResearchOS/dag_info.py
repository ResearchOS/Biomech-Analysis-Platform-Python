import os

import networkx as nx

from ResearchOS.custom_classes import Runnable

def is_package_in_packages(package_name: str) -> bool:
    """Check if a package is in the list of packages."""
    return package_name in os.environ['PACKAGE_NAMES'].split('.')

def is_runnable_in_package(dag: nx.MultiDiGraph, package_name: str, runnable_name: str) -> bool:
    """Check if a runnable is in a package."""
    full_runnable_name = package_name + '.' + runnable_name
    return len([node for node, _ in dag.nodes(data=True) if isinstance(dag.nodes[node]['node'], Runnable) and dag.nodes[node]['node'].name.startswith(full_runnable_name)]) > 0

def is_variable_in_runnable(dag: nx.MultiDiGraph, package_name: str, runnable_name: str, variable_name: str) -> bool:
    """Check if a variable is in a runnable."""
    full_variable_name = package_name + '.' + runnable_name + '.' + variable_name
    return len([node for _, node in dag.nodes(data=True) if node['node'].name == full_variable_name]) > 0

def check_variable_properly_specified(dag: nx.MultiDiGraph, package_name: str, runnable_name: str, variable_name: str) -> bool:
    """Check if a variable is properly specified."""
    if not is_package_in_packages(package_name):
        raise ValueError(f'Package {package_name} not in the list of packages.')
    if not is_runnable_in_package(dag, package_name, runnable_name):
        raise ValueError(f'Runnable {runnable_name} not in package {package_name}.')
    if not is_variable_in_runnable(dag, package_name, runnable_name, variable_name):
        raise ValueError(f'Variable {variable_name} not in runnable {runnable_name} in package {package_name}.')