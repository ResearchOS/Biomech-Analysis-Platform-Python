


def get_nodes_to_furcate(both_dags: dict) -> list:
    """Get the nodes in the DAG that need to be furcated.
    If an input variable has more than one source, then the node needs to be furcated."""
    nodes_to_furcate = []
    for target_node in both_dags['edges'].nodes:
        source_nodes = list(both_dags['edges'].predecessors(target_node))
        if len(source_nodes) > 1:
            nodes_to_furcate.append(target_node)
    return nodes_to_furcate