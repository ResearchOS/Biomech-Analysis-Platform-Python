
import networkx as nx

from ResearchOS.overhaul.custom_classes import InputVariable

def get_nodes_to_furcate(dag: nx.MultiDiGraph) -> list:
    """Get the nodes in the DAG that need to be furcated.
    If an input variable has more than one source, then the node needs to be furcated."""
    nodes_to_furcate = []
    variable_nodes = [node_id for node_id in dag.nodes if isinstance(dag.nodes[node_id]['node'], InputVariable)]
    for target_node_id in variable_nodes:
        # target_node = dag.nodes[target_node_id]['node']
        source_node_ids = list(dag.predecessors(target_node_id))
        source_nodes = [dag.nodes[source_node]['node'] for source_node in source_node_ids]
        if len(source_nodes) > 1:
            nodes_to_furcate.append(target_node_id)
    return nodes_to_furcate