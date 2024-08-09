import copy
import uuid

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

def polyfurcate(dag: nx.MultiDiGraph, nodes_to_furcate: list):
    """Furcate (split) the DAG downstream from each node."""
    for node_id in nodes_to_furcate:
        node = dag.nodes[node_id]['node']
        node_name = node.name # Until this point, node.name is guaranteed to be unique. After polyfurcation, it will not be unique.
        # Each predecessor should be linked to one copied DAG.
        predecessors = list(dag.predecessors(node_id)) # The multiple output variables for each of which a new DAG will be created
        assert len(predecessors) > 1, f"Node {node_id} has only one source. It should not be furcated."

        # Get all of the descendant nodes
        descendant_nodes = list(nx.descendants(dag, node_id)).append(node_id)        
        descendant_graph = dag.subgraph(descendant_nodes)

        # Create a new DAG for each source
        for predecessor in predecessors:
            new_dag, node_mapping = copy_dag(descendant_graph)

            # Add the new DAG to the overall DAG
            dag.add_nodes_from(new_dag.nodes(data=True))
            dag.add_edges_from(new_dag.edges)

            # Connect the non-furcating predecessor nodes to the new DAG
            # Get the predecessor nodes that feed into the subgraph in the original DAG.
            # Exclude nodes within the subgraph.
            non_furcate_predecessors = {}
            non_furcate_predecessors = {n:
                pred for n in descendant_graph.nodes
                for pred in dag.predecessors(n)
                if pred not in descendant_graph.nodes
            }            
            
            for pred, orig_node in non_furcate_predecessors.items():
                new_node = node_mapping[orig_node]
                dag.add_edge(pred, new_node)

            # Connect the furcating predecessor to the new DAG
            new_node = [n for n in new_dag.nodes if new_dag.nodes[n]['node'].name == node_name][0]
            dag.add_edge(predecessor, new_node)
    return dag

        
def copy_dag(original_dag: nx.MultiDiGraph) -> nx.MultiDiGraph:
    """Copy the DAG with new node UUID's, preserving (deep copying) the node data."""
    original_edges = original_dag.edges
    
    new_dag = nx.MultiDiGraph()

    # Mapping from the original node UUID to the new node UUID
    node_mapping = {node: str(uuid.uuid4()) for node in original_dag.nodes}

    # Add nodes with new UUID's and deep copy of data
    for old_node, new_node in node_mapping.items():
        original_data = original_dag.nodes[old_node]['node']
        copied_data = copy.deepcopy(original_data)
        copied_data["id"] = new_node # Change the UUID
        new_dag.add_node(new_node, node=copied_data)

    # Add edges with new UUID's
    new_dag.add_edges_from((node_mapping[u], node_mapping[v]) for u, v in original_edges)

    return new_dag, node_mapping