from collections import defaultdict

import networkx as nx
import matplotlib.pyplot as plt

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
    layers = get_layers(dag)
    # Assign positions based on layers
    pos = {}
    layer_height = 8.0  # Vertical space between layers
    layer_width = 1.0   # Horizontal space between nodes in the same layer

    for layer, nodes in layers.items():
        for i, node in enumerate(nodes):
            pos[node] = (i * layer_width, -layer * layer_height)

    label_pos = {}
    for layer, nodes in layers.items():
        n = len(nodes)
        step = layer_height / (n - 1) if n > 1 else 0
        for i, node in enumerate(nodes):
            # offset = (i - (n - 1) / 2) * (layer_height/10)  # Calculate the vertical offset
            label_pos[node] = (pos[node][0], pos[node][1] + ((layer_height/2) - i*step))

    # label_pos = {node: (x, y - 0.1) for node, (x, y) in pos.items()}
    nx.draw(dag, pos, with_labels=False, labels=labels)
    nx.draw_networkx_labels(dag, label_pos, labels, font_size=8)
    plt.show()