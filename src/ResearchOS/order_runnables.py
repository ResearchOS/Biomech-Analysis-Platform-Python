"""These functions take in a fully compiled DAG and a dict where the keys are the node names and their values are their orders."""

import networkx as nx

from ResearchOS.custom_classes import Runnable

def order_nodes(dag: nx.MultiDiGraph, start_node_name: str = None) -> nx.MultiDiGraph:
    """Order the nodes."""
    dag = nx.transitive_closure(dag) # So that I can properly use subgraph.
    dag_to_run = dag # Default: Include all nodes.
    nodes_in_dag = [node for node in dag.nodes] # List of all of the node UUID's in the DAG.
    topo_sorted_nodes = {node: None for node in nodes_in_dag} # Default: No nodes are being run.
    if start_node_name:
        nodes_in_dag = [] # Reset the list of nodes in the DAG.
        # NOTE: Due to polyfurcation, there may be more than one node identified as the start_node. This is ok!
        # Get the UUID of the start node
        start_nodes = [node for _, node in dag.nodes if node['node'].name == start_node_name and isinstance(node['node'], Runnable)]
        if not start_nodes:
            raise ValueError(f"Specified Runnable node {start_node_name} not found in the DAG.")
        # Get all of the downstream nodes (Runnable & Variable)
        for start_node in start_nodes:
            nodes_in_dag.append(list(nx.descendants(dag, start_node)))
            nodes_in_dag.append(start_node)
        nodes_in_dag = [node for node in nodes_in_dag if isinstance(nodes_in_dag["node"], Runnable)]
        dag_to_run = dag.subgraph(nodes_in_dag)

    tmp_topo_sorted_nodes = {node: order for order, node in enumerate(list(nx.topological_sort(dag_to_run)))}
    topo_sorted_nodes.update(tmp_topo_sorted_nodes) # Assign the proper orders to the nodes.
    return topo_sorted_nodes
