from copy import deepcopy
import networkx as nx

from ResearchOS.custom_classes import DataFilePath, LoadConstantFromFile, DataObjectName

def resolve_dag(dag: nx.MultiDiGraph, data_object: list) -> nx.MultiDiGraph:
    """Resolve the DAG for the currently specified data object.
    This means that each input variable node that is loaded from file, uses the data object's name, etc.
    Is converted from the generic form to that of the specific data object."""
    new_dag = deepcopy(dag(data=True)) # data set to True so that the new graph is completely independent of the old one.
    node_types_to_resolve = (DataFilePath, LoadConstantFromFile, DataObjectName)
    for node in dag.nodes:
        node_obj = new_dag.nodes[node]['node']
        if type(node_obj) in node_types_to_resolve:
            node_obj.resolve(data_object)
        new_dag.nodes[node]['node'] = node_obj
    return new_dag