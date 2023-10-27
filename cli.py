import typer
from pathlib import Path

import sys
app = typer.Typer()

import networkx as nx
from networkx.drawing.nx_agraph import graphviz_layout, to_agraph
from graphviz import Digraph

@app.command(name="name_later")
def name_later():
    pass

def build():    
    graph = nx.DiGraph()


def draw_graph(graph: nx.DiGraph):
    A = to_agraph(graph)
    A.layout('dot')
    A.draw('foo.png')

    


if __name__ == "__main__":
    app()