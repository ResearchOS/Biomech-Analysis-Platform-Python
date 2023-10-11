import networkx as nx
import sqlite3

G = nx.Graph()
G.add_edges_from([(1,2), (1,3)])
nx.draw(G)

G = nx.complete_graph(5)
nx.draw(G)
print('Stop here')