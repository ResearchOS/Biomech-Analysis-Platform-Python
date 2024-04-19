import json
import time
import copy
import pickle

import typer
from typer.testing import CliRunner
import toml
import networkx as nx

from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.config import Config
from ResearchOS.cli.quickstart import create_folders
from ResearchOS.db_initializer import DBInitializer
from ResearchOS.action import Action, logger
from ResearchOS.tomlhandler import TOMLHandler
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.cli_entrypoint import add_src_to_path
from ResearchOS.cli.get_all_paths_of_type import import_objects_of_type
from ResearchOS.build_pl import build_pl

app = typer.Typer()



add_src_to_path()

def read_pl():
    # with open("pipeline.pkl","rb") as f:
    #     G = pickle.load(f)
    with open("pl_node.pkl","rb") as f:
        pl_node = pickle.load(f)
    return pl_node

def write_pl(pl_node):
    # with open("pipeline.pkl","wb") as f:
    #     pickle.dump(G,f)
    if not isinstance(pl_node, str):
        pl_node = pl_node.id
    with open("pl_node.pkl","wb") as f:
        pickle.dump(pl_node,f)
    

@app.command()
def start(plobj_id: str = typer.Argument(help="The pipeline object to start at.",default=None)):
    """Start the pipeline."""
    from ResearchOS.DataObjects.data_object import DataObject
    G = build_pl()
    subclasses = DataObject.__subclasses__()
    if plobj_id is None:
        sorted_nodes = list(nx.topological_sort(G))
        pl_node = sorted_nodes[0]
    else:
        cls = [cls for cls in subclasses if cls.prefix == plobj_id[:2]][0]
        pl_node = cls(id=plobj_id)
    write_pl(pl_node)
    
@app.command()
def print_node():
    """Print the pipeline. Show the connections to the neighbor nodes."""
    from ResearchOS.DataObjects.data_object import DataObject
    from ResearchOS.research_object import ResearchObject
    pl_node = read_pl()
    G = build_pl()
    subclasses = ResearchObjectHandler._get_subclasses(ResearchObject)
    cls = [cls for cls in subclasses if hasattr(cls, "prefix") and cls.prefix == pl_node[:2]][0]
    action = Action(name="Print_Node")
    pl_node = cls(id=pl_node, action=action)
    # Get the edges associated with this node.
    edges = [e for e in G.edges(data=True) if e[0] == pl_node or e[1] == pl_node]
    print(f"Inputs to {pl_node.id}:")
    for edge in edges:
        if edge[1] == pl_node:
            edge[2]["edge"].output.parent_ro = pl_node # Temporary hacky fix       
            print(edge[2]["edge"])
    print(f"Outputs from {pl_node.id}:")
    for edge in edges:
        if edge[0] == pl_node:
            edge[2]["edge"].output.parent_ro = pl_node # Temporary hacky fix
            print(edge[2]["edge"])

@app.command()
def move(next_node: str = typer.Argument(help="The next node to move to.")):
    """Move to the next node in the pipeline."""
    write_pl(next_node)
    print_node()

@app.command()
def dobj(dobj_id: str = typer.Argument(help="The data object to evaluate.",default=None)):
    """Create a data object."""
    from ResearchOS.DataObjects.data_object import DataObject
    action = Action(name="Create_DataObject")
    if dobj_id is None:
        dobj = DataObject(action=action)
    else:
        dobj = DataObject(id=dobj_id,action=action)
    write_pl(dobj)
    print_node()
    
if __name__=="__main__":
    app(["print-node"])