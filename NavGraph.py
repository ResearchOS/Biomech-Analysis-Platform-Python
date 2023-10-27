from networkx import DiGraph
from networkx.drawing.nx_agraph import graphviz_layout, to_agraph
from graphviz import Digraph
import inquirer

class NavGraph(DiGraph):
    def __init__(self, fcn: callable, db_file: str, digraph: DiGraph = DiGraph()):
        super().__init__()
        if not digraph:
            self.build(fcn, db_file)   

    def build(self, fcn: callable, db_file: str) -> DiGraph:
        if not fcn or not db_file:
            return DiGraph # Empty graph
                
        digraph = fcn(db_file)
        
        # Check that the returned value is a nx.DiGraph
        if type(digraph) != DiGraph:
            raise TypeError("The function must return a networkx.DiGraph.")

        # Add the node and edge attributes to the NavGraph
        self.add_nodes_from(digraph.nodes)
        for node in digraph.nodes:
            for attr in digraph.nodes[node]:
                self.nodes[node][attr] = digraph.nodes[node][attr]        
        self.add_edges_from(digraph.edges)
        for edge in digraph.edges:
            for attr in digraph.edges[edge]:
                self.edges[edge][attr] = digraph.edges[edge][attr]
        return digraph
    
    def draw(self) -> None:
        """ Draws the graph in a separate window. Lists the nodes in the terminal for navigation."""
        # Draw with PyGraphViz
        A = to_agraph(self)
        A.layout('dot')
        A.draw('foo.png')

        nodes = self.get_neighborhood(self.selected_node)
        inquirer.list_input("Select a node to view its predecessors and successors", choices=nodes)

        # TODO: Print the nodes in the terminal
        # for node, i in enumerate(digraph.nodes):
        #     print(i + ": " + node)

    def select_node(self, uuid: str) -> str:
        """ Selects a node from the graph."""
        self.selected_node = uuid

        # Draw the predecessors and successors of the selected node.
        nodes = self.get_neighborhood()
        self.plotted_digraph = self.subgraph(nodes)
        self.draw(self.plotted_digraph)

    def get_neighborhood(self) -> list:
        """ Returns a list of the predecessors and successors of the selected node, and the selected node itself."""
        preds = self.predecessors(self.selected_node)
        succs = self.successors(self.selected_node)

        nodes = []
        nodes.append(self.selected_node)
        nodes.append(preds)
        nodes.append(succs)
        return nodes


    

if __name__ == "__main__":
    pass