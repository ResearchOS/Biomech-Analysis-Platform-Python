

import networkx as nx

import read_toml
import make_config

def get_pkg_deps_graph(pkg_name: str, G: nx.MultiDiGraph = nx.MultiDiGraph()) -> nx.MultiDiGraph:
    """Return the dependency graph of the package."""
    pkg_pyproject_path = read_toml.get_pkg_pyproject_toml_file_path(pkg_name)
    toml_dict = read_toml.read_toml(pkg_pyproject_path)
    deps_list = read_toml.get_deps_list(toml_dict)

    # Depth first, recursively add the dependencies to the graph.
    for dep in deps_list:
        dep_name = read_toml.parse_dep_for_pkg_name(dep)
        G.add_node(dep_name)        
        G.add_edge(pkg_name, dep_name)

        get_pkg_deps_graph(dep_name, G)

    [G[pkg].update({cls: {}}) for pkg in G for cls in make_config.class_names] # Initialize each node with the class names.
    return G

def sort_topo_alpha(G: nx.MultiDiGraph) -> list:
    """Return a topological sort of the graph. Within each generation, the nodes are in alphabetical order for idempotency."""
    # Get the topological sort of the graph.
    topo_sorted = list(nx.topological_sort(G))

    # Get the generation level of each node.
    levels = nx.single_source_shortest_path_length(G.reverse(), topo_sorted[0])

    # Group nodes by their generation level.
    generations = {}
    for node, level in levels.items():
        if level not in generations:
            generations[level] = []
        generations[level].append(node)

    # Sort nodes within each generation alphabetically
    sorted_generations = []
    for level in sorted(generations.keys()):
        sorted_generations.extend(sorted(generations[level]))

    return sorted_generations