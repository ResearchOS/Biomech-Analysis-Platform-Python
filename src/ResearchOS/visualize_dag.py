from collections import defaultdict
from copy import deepcopy

import networkx as nx
import matplotlib.pyplot as plt

from ResearchOS.custom_classes import Constant, InputVariable, OutputVariable, Unspecified, Logsheet, Process, Runnable, Variable, Dynamic

colors_dict = {
    Constant: 'blue',
    InputVariable: 'green',
    OutputVariable: 'red',
    Unspecified: 'yellow',
    Logsheet: 'cyan',
    Process: 'purple'    
}

def get_layers(graph):
    layers = defaultdict(list)
    for node in nx.topological_sort(graph):
        layer = 0
        for predecessor in graph.predecessors(node):
            layer = max(layer, layers[predecessor] + 1)  # Determine the layer of the node
        layers[node] = layer
    # Invert the layers dictionary to get nodes per layer
    layers_by_generation = defaultdict(list)
    for node, layer in layers.items():
        layers_by_generation[layer].append(node)
    return layers_by_generation

def visualize_dag(dag: nx.MultiDiGraph, layout: str = 'generation'):    
    labels = {n: data['node'].name for n, data in dag.nodes(data=True)}
    labels = {k: v.replace('.', '.\n') for k, v in labels.items()}
    layers = get_layers(dag)
    # Assign positions based on layers    
    layer_height = 1.0  # Vertical space between layers
    layer_width = 1.0   # Horizontal space between nodes in the same layer

    if layout == 'generation':
        pos, label_pos = set_generational_layout(dag, layers, layer_width, layer_height)
    else:
        pos, label_pos = set_topological_layout(dag, layer_width, layer_height)

    node_colors = [colors_dict[dag.nodes[node]['node'].__class__] if dag.nodes[node]['node'].__class__ in colors_dict.keys() else 'black' for node in dag.nodes]

    # Strip package and function names on variables to make the graph more readable
    for label in labels:
        node = [node[1]['node'] for node in dag.nodes(data=True) if node[0] == label][0]
        if isinstance(node, Variable):
            labels[label] = node.name.split('.')[-1]

    nx.draw(dag, pos, with_labels=False, labels=labels, node_color=node_colors, edge_color='grey')
    nx.draw_networkx_labels(dag, label_pos, labels, font_size=8)
    # plt.show()

def get_sorted_runnable_nodes(dag: nx.MultiDiGraph):
    # 1. Sort all of the Runnable nodes in the DAG by their topological sort.
    runnable_nodes = [node for node in dag.nodes(data=True) if isinstance(node[1]['node'], Runnable)]
    trans_clos_dag = nx.transitive_closure(dag)
    runnables_dag = trans_clos_dag.subgraph([node[0] for node in runnable_nodes])
    sorted_runnable_nodes = [node for node in nx.topological_sort(runnables_dag)]

    # For the sorted Runnable nodes, rearrange such that any Runnable with only Constant or Unspecified inputs is moved just before the first Runnable that uses its output.
    runnable_nodes_with_constant_or_unspecified_inputs = list(reversed([node for node in sorted_runnable_nodes if all([isinstance(dag.nodes[pred]['node'], (Constant, Unspecified)) for pred in list(dag.predecessors(node))])]))
    runnable_node_names_with_constant_or_unspecified_inputs = [dag.nodes[node]['node'].name for node in runnable_nodes_with_constant_or_unspecified_inputs]
    for node in runnable_nodes_with_constant_or_unspecified_inputs:
        if isinstance(dag.nodes[node]['node'], Logsheet):
            continue
        node_name = dag.nodes[node]['node'].name
        descendants = list(nx.descendants(runnables_dag, node)) + [node]
        descendants_names = [dag.nodes[descendant]['node'].name for descendant in descendants]
        for d in descendants:
            if not all([type(dag.nodes[pred]['node']) == InputVariable for pred in list(dag.predecessors(d))]):
                sorted_runnable_nodes.remove(node)
                # Get the index of the first Runnable successor of this descendant.
                min_successor_runnable_index = len(sorted_runnable_nodes)+1
                descendants2 = list(nx.descendants(runnables_dag, d))
                for d2 in descendants2:
                    min_successor_runnable_index = min(min_successor_runnable_index, sorted_runnable_nodes.index(d2))
                sorted_runnable_nodes.insert(min_successor_runnable_index-1, node) 
    return sorted_runnable_nodes

def set_topological_layout(dag: nx.MultiDiGraph, layer_width: float, layer_height: float):
    """Left to right layout"""
    sorted_runnable_nodes = get_sorted_runnable_nodes(dag)       

    pos = {}    
    for i, node in enumerate(sorted_runnable_nodes):
        pos[node] = (3 * i * layer_width, 0) # Times 3 because inputs and outputs need to go between function nodes.

        inputs = list(dag.predecessors(node))
        input_step = layer_height / (len(inputs) - 1) if len(inputs) > 1 else 0
        input_offsets = [-layer_height/2 + k * input_step for k in range(len(inputs))]          
        outputs = list(dag.successors(node))
        output_step = layer_height / (len(outputs) - 1) if len(outputs) > 1 else 0
        output_offsets = [-layer_height/2 + k * output_step for k in range(len(outputs))]
        for j, input_node in enumerate(inputs):            
            pos[input_node] = (pos[node][0] - 1, input_offsets[j])
        for j, output_node in enumerate(outputs):
            pos[output_node] = (pos[node][0] + 1, output_offsets[j])

    label_pos = deepcopy(pos)
    return pos, label_pos

def set_generational_layout(dag: nx.MultiDiGraph, layers: list, layer_width: float, layer_height: float):
    """Top to bottom layout"""
    # Move constants to the layer below the lowest layer of their successors
    pos = {}
    first_layer = deepcopy(layers[0])
    for node in first_layer:
        if not isinstance(dag.nodes[node]['node'], (Constant, Unspecified)):
            continue
        min_layer = len(layers)
        successors = list(dag.successors(node))            
        for successor in successors:
            for layer_num, layer in enumerate(layers):
                if successor in layers[layer_num]:
                    min_layer = min(min_layer, layer_num)
                    break
        layers[0].remove(node)
        layers[min_layer-1].append(node)        

    for layer, nodes in layers.items():
        for i, node in enumerate(nodes):
            pos[node] = (i * layer_width, -layer * layer_height)

    label_pos = {}
    for layer, nodes in layers.items():
        for i, node in enumerate(nodes):
            mod = i % 2
            label_pos[node] = (pos[node][0], pos[node][1])
            if mod == 0:
                label_pos[node] = (pos[node][0], pos[node][1] - 0.1*layer_height)
            else:
                label_pos[node] = (pos[node][0], pos[node][1] + 0.1*layer_height) 

    return pos, label_pos