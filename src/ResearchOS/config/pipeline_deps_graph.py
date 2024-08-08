import os

import networkx as nx

def get_pipeline_deps_graph():
    """Get the pipeline dependencies graph.
    Each node is named {pkg_name}.{class_name}.{obj_name}."""
    G_pkg = os.environ["RESEARCHOS"]["PKG_GRAPH"]

    # Create the pipeline dependencies graph and initialize the nodes.
    G = nx.MultiDiGraph()
    for pkg in G_pkg.nodes():
        for class_name in G_pkg[pkg].keys():
            for obj in G_pkg[pkg][class_name].keys():
                G.add_node(f"{pkg}.{class_name}.{obj}", pkg_name=pkg, class_name=class_name, obj_name = obj)

    # Add the edges to the pipeline dependencies graph.
    for node in G.nodes():
        if "inputs" in node.keys():
            for input_node in node["inputs"]:
                # Parse input_node to get the package, class_name, and obj.
                obj_name, class_name, pkg_name = parse_input_node(input_node, node["pkg_name"], node["class_name"])
                src_node = f"{pkg_name}.{class_name}.{obj_name}"
                G.add_edge(src_node, node)

    return G


def parse_input_node(input_node: str, current_pkg: str, current_class_name: str) -> tuple:
    """Parse the input node to get the package, class_name, and obj.
    Returned in order from smallest to largest scope (obj, class_name, package)."""
    split_list = list(reversed(input_node.split("."))) # Reversed so the order is obj, class_name, pkg.
    # If length is 1, use current package and class_name.
    if len(split_list)==1:
        split_list += [current_class_name, current_pkg]
    elif len(split_list)==2:
        split_list += [current_pkg]

    return tuple(split_list)