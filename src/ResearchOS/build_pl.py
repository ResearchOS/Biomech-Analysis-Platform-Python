from typing import TYPE_CHECKING
import json

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

import networkx as nx

from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.Digraph.pipeline_digraph import PipelineDiGraph
from ResearchOS.cli.get_all_paths_of_type import import_objects_of_type
from ResearchOS.action import Action
from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.variable import Variable




     