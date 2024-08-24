"""These functions take in a fully compiled DAG and a dict where the keys are the node names and their values are their orders."""

import networkx as nx

from ResearchOS.custom_classes import Runnable

def order_runnable_nodes(runnables_dag: nx.MultiDiGraph, start_node_name: str = None) -> nx.MultiDiGraph:
    """Order the nodes."""
    dag_to_run = runnables_dag # Default: Include all nodes.
    nodes_in_dag = [node for node in dag_to_run.nodes] # List of all of the node UUID's in the DAG.
    topo_sorted_nodes = [] # Default: No nodes are being run.
    if start_node_name:
        nodes_in_dag = [] # Reset the list of nodes in the DAG.
        # NOTE: Due to polyfurcation, there may be more than one node identified as the start_node. This is ok!
        # Get the UUID of the start node
        start_nodes = [node for _, node in dag_to_run.nodes if node['node'].name == start_node_name and isinstance(node['node'], Runnable)]
        if not start_nodes:
            raise ValueError(f"Specified Runnable node {start_node_name} not found in the DAG.")
        # Get all of the downstream Runnable nodes
        for start_node in start_nodes:
            nodes_in_dag.append(list(nx.descendants(dag_to_run, start_node)))
            nodes_in_dag.append(start_node)
        nodes_in_dag = [node for node in nodes_in_dag if isinstance(nodes_in_dag["node"], Runnable)]
        dag_to_run = dag_to_run.subgraph(nodes_in_dag)

    topo_sorted_nodes = [node for node in list(nx.topological_sort(dag_to_run))]
    return topo_sorted_nodes

def get_dag_of_runnables(dag: nx.MultiDiGraph) -> nx.MultiDiGraph:
    """Given a DAG with variables & Runnables, return a DAG with only Runnable nodes.
    This DAG has the advantage of being able to topologically sort the Runnable nodes."""
    # Get the transitive closure
    trans_clos_dag = nx.transitive_closure_dag(dag)
    runnable_nodes = [node for node in dag.nodes(data=True) if isinstance(node[1]["node"], Runnable)]
    return trans_clos_dag.subgraph(runnable_nodes).copy()