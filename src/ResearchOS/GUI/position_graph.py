import json, os

import networkx as nx

def write_js_graph(G: nx.Graph, pos = None) -> None:
    """Write the graph data to a .js file."""
    # Position layout
    if not pos:
        # Bipartite layout.
        for layer, nodes in enumerate(nx.topological_generations(G)):
            for node in nodes:
                G.nodes[node]['layer'] = layer
        pos = nx.multipartite_layout(G, subset_key='layer')

    # Extract nodes and edges with the required format
    nodes = [{'id': str(node), 'x': round(pos[node][0], 2), 'y': round(pos[node][1], 2)} for node in G.nodes]
    edges = [{'id': str(edge[2]), 'from': str(edge[0]), 'to': str(edge[1])} for edge in G.edges]

    # JavaScript object as a string
    js_data = f"const graphData = {{\n    nodes: {json.dumps(nodes, indent=8)},\n    edges: {json.dumps(edges, indent=8)}\n}};"

    # Writing to a .js file
    folder = os.path.dirname(os.path.abspath(__file__))
    graph_data_js = os.path.join(folder, "graph-data.js")
    with open(graph_data_js, 'w') as file:
        file.write(js_data)

    print("Data has been written to graphData.js")

    html_path = os.path.join(folder, "plot_graph.html")
    return html_path