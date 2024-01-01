import networkx as nx
from src.ResearchOS.research_object import ResearchObject

from src.ResearchOS.action import Action

class ResearchObjectDigraph(nx.MultiDiGraph):
    """Research Object Digraph."""

    def __init__(self):
        # 1. Load the list of source and target nodes from the database.
        super().__init__() # So that this object is properly initialized as a MultiDiGraph.
        sqlquery = "SELECT object_id, target_object_id FROM research_object_attributes WHERE target_object_id IS NOT NULL"
        nodes = Action.conn.cursor().execute(sqlquery).fetchall()
        source_object_ids = [node[0] for node in nodes]
        target_object_ids = [node[1] for node in nodes]

        # 2. Create the graph.
        for node_num in range(len(source_object_ids)):
            self.add_edge(source_object_ids[node_num], target_object_ids[node_num])

    def __getitem__(self, key: str) -> ResearchObject:        
        """Called (hopefully) every time an object is provided as a key to the Digraph.
        Loads that object from the database."""
        # 1. Check that the key is one of the nodes in the digraph.
        if key not in self.nodes:
            ValueError("Key is not a node in the digraph!")

        # 2. Load it from the database. How to get the class?
        return cls(id = key)
    
if __name__=="__main__":
    rod = ResearchObjectDigraph()
    G = nx.MultiDiGraph()
    G.add_node(1)
    rod.add_node(1)
    nodes = G.nodes()
    print(G.nodes)