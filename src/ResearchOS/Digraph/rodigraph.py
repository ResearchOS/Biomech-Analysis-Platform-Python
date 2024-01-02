import networkx as nx
from src.ResearchOS.research_object import ResearchObject

from src.ResearchOS.action import Action

import json

class ResearchObjectDigraph(nx.MultiDiGraph):
    """Research Object Digraph."""

    def __init__(self):
        # NOTE: I know that this is not the most efficient algorithm to do this, but it'll work for small scale.        
        # 1. Initialize the digraph.
        super().__init__() # So that this object is properly initialized as a MultiDiGraph.

        # 2. Load all of the nodes from the database. Assumes that they are sorted in ascending time order.
        sqlquery = f"SELECT attr_id FROM attributes WHERE attr_name IS '{ResearchObject.DEFAULT_EXISTS_ATTRIBUTE_NAME}'"
        result = Action.conn.cursor().execute(sqlquery).fetchall()
        if result is None:
            return # Empty digraph anyways.        
        exists_attr_id = result[0][0] # Will most likely be 1, but is potentially subject to change.
        sqlquery = f"SELECT object_id, attr_value FROM research_object_attributes WHERE attr_id = '{exists_attr_id}'"
        all_nodes = Action.conn.cursor().execute(sqlquery).fetchall()      
        object_ids = [node[0] for node in all_nodes]
        exists_values = [node[1] for node in all_nodes]
        for row, node in enumerate(object_ids):
            if json.loads(exists_values[row]) == True:
                self.add_node(node)
            elif json.loads(exists_values[row]) == False:
                self.remove_node(node)

        # 2. Load the list of source and target nodes from the database.
        sqlquery = f"SELECT object_id, target_object_id, attr_value FROM research_object_attributes WHERE target_object_id IS NOT NULL AND attr_id = '{exists_attr_id}'"
        nodes = Action.conn.cursor().execute(sqlquery).fetchall()        
        source_object_ids = [node[0] for node in nodes]
        target_object_ids = [node[1] for node in nodes]     
        exists_values = [node[2] for node in nodes]

        # 3. Create the graph edges.
        for row, node in enumerate(source_object_ids):
            source_object_id = source_object_ids[row]
            target_object_id = target_object_ids[row]
            if json.loads(exists_values[row]) == True:
                self.add_edge(source_object_id, target_object_id)
            elif json.loads(exists_values[row]) == False:
                self.remove_edge(source_object_id, target_object_id)

    def __getitem__(self, key: str) -> ResearchObject:        
        """Called every time an object is provided as a key to the Digraph.
        Loads that object from the database."""        
        # 1. Check that the key is one of the nodes in the digraph.
        if key not in self.nodes:
            ValueError("Key is not a node in the digraph!")

        # 2. Load it from the database. How to get the class?
        return cls(id = key)
    
if __name__=="__main__":
    rod = ResearchObjectDigraph()
    rod.add_node("LG000000_001")
    nodes = rod.nodes()
    print(nodes)
    lg = rod["LG000000_001"]
    print(lg.num_header_rows)