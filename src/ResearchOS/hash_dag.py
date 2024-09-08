import os
import math
import json
from hashlib import sha256
from typing import Any

import networkx as nx

from ResearchOS.custom_classes import Node, Runnable, Variable

def get_attrs_to_hash(node: Node) -> tuple:
    if isinstance(node, Runnable):
        attrs_to_hash_no_data_object = ['function', 'level', 'batch', 'subset']
        attrs_to_hash_data_object = ['function', 'level', 'batch']
    elif isinstance(node, Variable):
        attrs_to_hash_no_data_object = ['unresolved_value','slices']
        attrs_to_hash_data_object = ['resolved_value','slices']
    return attrs_to_hash_no_data_object, attrs_to_hash_data_object

def hash_node(dag: nx.MultiDiGraph, node: str, iterations: int = 10, data_object: str = None) -> str:
    """Hash a node, given a DAG that has either:
    1. Been resolved for a particular data object (in which case data_object input is provided)
    2. Not been resolved for a particular data object (in which case data_object input is None)"""
    # Get the ancestors of the node
    ancestors = list(nx.ancestors(dag, node))
    ancestors.append(node)
    ancestors_graph = dag.subgraph(ancestors)
    # Update and move the attribute for hashing to the DAG    
    for node_uuid in ancestors_graph.nodes:
        node = ancestors_graph.nodes[node_uuid]['node']
        attrs_to_hash_no_data_object, attrs_to_hash_data_object = get_attrs_to_hash(node)
        node.set_attr_for_hashing(attrs_to_hash_no_data_object, attrs_to_hash_data_object, node.attrs, data_object)                
        ancestors_graph.nodes[node_uuid]['attr_for_hashing'] = node.attr_for_hashing
    node_hashes = nx.weisfeiler_lehman_subgraph_hashes(ancestors_graph, node_attr="attr_for_hashing", iterations=iterations)
    # Remove the attribute
    for node in ancestors_graph.nodes:
        del ancestors_graph.nodes[node]['attr_for_hashing']

    # Sorting the hashes to ensure consistent ordering across runs
    combined_string = ''.join(sorted([str(node_hash) for node_hash in node_hashes.values()]))

    # Hash the combined string using SHA-256 (or any preferred hash function)
    return sha256(combined_string.encode('utf-8')).hexdigest()
