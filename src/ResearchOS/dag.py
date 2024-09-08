# import networkx as nx

# class DAG(nx.MultiDiGraph):
#     """A custom Directional Acylic Graph (DAG) class that inherits from networkx's MultiDiGraph.
#     The customization provides the following benefits:
#     1. Easier hashing?"""
#     def __init__(self, nodes: list = [], edges: list = [], **attrs):
#         # Initialize the base class
#         super().__init__()
#         self.add_nodes_from(nodes)
#         self.add_edges_from(edges)
#         self.attrs = attrs

#     def add_node(self, node_for_adding, **attr):
#         super().add_node(node_for_adding, **attr)

#     def add_nodes_from(self, nodes_for_adding, **attr):
#         super().add_nodes_from(nodes_for_adding, **attr)

#     def add_edge(self, u_of_edge, v_of_edge, **attr):
#         super().add_edge(u_of_edge, v_of_edge, **attr)

#     def add_edges_from(self, ebunch_to_add, **attr):
#         super().add_edges_from(ebunch_to_add, **attr)