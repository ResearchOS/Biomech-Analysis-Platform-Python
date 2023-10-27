import typer
from pathlib import Path

import sys
app = typer.Typer()

import networkx as nx
from NavGraph import NavGraph
from load_digraph import load_digraph

@app.command(name="build")
def build(fcn: callable, db_file: str) -> NavGraph:   
    graph = NavGraph(fcn, db_file)
    return graph

@app.command(name="sel")
def select_node(graph: NavGraph, input: str) -> None:
    # Select the specified node (can be predecessor or successor)
    uuid = input.to_number
    graph.select_node(uuid) 

@app.command(name="fav")
def show_favorites(graph: NavGraph) -> None:
    # TODO: Load the favorite nodes for this digraph.
    pass

@app.command(name="fav-set")
def fav_set():
    # Set a new favorite node
    pass

@app.command(name="fav-rem")
def fav_rem():
    # Remove a favorite node from the list.
    pass


if __name__ == "__main__":
    # app()
    db_file = "/Users/mitchelltillman/Desktop/Work/MATLAB_Code/GitRepos/Biomech-Analysis-Platform-MATLAB/Databases/biomechOS.db"    
    graph = NavGraph(load_digraph, db_file)
    print(graph.nodes)