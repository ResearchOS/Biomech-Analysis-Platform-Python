from collections import defaultdict
from copy import deepcopy

import networkx as nx
import matplotlib.pyplot as plt

from ResearchOS.custom_classes import Constant, InputVariable, OutputVariable, Unspecified, Logsheet, Process

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

def visualize_dag(dag):    
    labels = {n: data['node'].name for n, data in dag.nodes(data=True)}
    labels = {k: v.replace('.', '.\n') for k, v in labels.items()}
    layers = get_layers(dag)
    # Assign positions based on layers
    pos = {}
    layer_height = 1.0  # Vertical space between layers
    layer_width = 1.0   # Horizontal space between nodes in the same layer

    # Move constants to the layer below the lowest layer of their successors
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
        layers[min_layer-1].append(node)
        layers[0].remove(node)

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

    node_colors = [colors_dict[dag.nodes[node]['node'].__class__] if dag.nodes[node]['node'].__class__ in colors_dict.keys() else 'black' for node in dag.nodes]

    nx.draw(dag, pos, with_labels=False, labels=labels, node_color=node_colors, edge_color='grey')
    nx.draw_networkx_labels(dag, label_pos, labels, font_size=8)
    # plt.show()