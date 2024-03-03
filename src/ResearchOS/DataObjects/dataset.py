from typing import Any
import json, copy, os

import networkx as nx

# from ResearchOS.Graphs.data_graph import DataGraph
# from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.sqlite_pool import SQLiteConnectionPool

all_default_attrs = {}
all_default_attrs["schema"] = [] # Dict of empty dicts, where each key is the class and the value is a dict with subclass type(s).
all_default_attrs["dataset_path"] = None # str
all_default_attrs["addresses"] = [] # Dict of empty dicts, where each key is the ID of the object and the value is a dict with the subclasses' ID's.

computer_specific_attr_names = ["dataset_path"]

class Dataset(DataObject):
    """A dataset is one set of data.
    Class-specific Attributes:
    1. data path: The root folder location of the dataset.
    2. data schema: The schema of the dataset (specified as a list of classes)"""

    prefix: str = "DS"
    
    ### Schema Methods
        
    def validate_schema(self, schema: list, action: Action, default: Any) -> None:
        """Validate that the data schema follows the proper format.
        Must be a dict of dicts, where all keys are Python types matching a DataObject subclass, and the lowest levels are empty."""
        from ResearchOS.research_object import ResearchObject
        if schema == default:
            return
        subclasses = ResearchObject.__subclasses__()
        dataobj_subclasses = DataObject.__subclasses__()
        vr = [x for x in subclasses if hasattr(x,"prefix") and x.prefix == "VR"][0]                
            
        graph = nx.MultiDiGraph()
        try:
            graph.add_edges_from(schema)
        except nx.NetworkXError:
            raise ValueError("The schema must be provided as an edge list!")
        if not nx.is_directed_acyclic_graph(graph):
            raise ValueError("The schema must be a directed acyclic graph!")
        
        non_subclass = [node for node in graph if node not in dataobj_subclasses]
        if non_subclass:
            raise ValueError("The schema must only include DataObject subclasses!")
        
        if Dataset not in graph:
            raise ValueError("The schema must include the Dataset class as a source node!")
        
        if vr in graph:
            raise ValueError("The schema must not include the Variable class as a target node!")
        
        # nodes_with_no_targets = [node for node, out_degree in graph.out_degree() if out_degree == 0]
        # nodes_with_a_source = [node for node, in_degree in graph.in_degree() if in_degree > 0]
        # if graph[Dataset] in nodes_with_no_targets or graph[Dataset] in nodes_with_a_source:
        #     raise ValueError("The schema must include the Dataset class as a source node and not a target node!")

    def save_schema(self, schema: list, action: Action) -> None:
        """Save the schema to the database."""
        # 1. Convert the list of types to a list of str.
        str_schema = []
        for sch in schema:
            classes = []
            for cls in sch:
                classes.append(cls.prefix)
            str_schema.append(classes)
        # 2. Convert the list of str to a json string.
        json_schema = json.dumps(str_schema)

        # 3. Save the schema to the database.        
        schema_id = IDCreator(action.conn).create_action_id()
        # sqlquery = f"INSERT INTO data_address_schemas (schema_id, levels_edge_list, dataset_id, action_id) VALUES (?, ?, ?, ?)"
        params = (schema_id, json_schema, self.id, action.id)
        action.add_sql_query(self.id, "data_address_schemas_insert", params, group_name = "robj_complex_attr_insert")

    def load_schema(self, action: Action) -> list:
        """Load the schema from the database and convert it via json."""
        # 1. Get the dataset ID
        id = self.id
        # 2. Get the most recent action ID for the dataset in the data_address_schemas table.
        schema_id = self.get_current_schema_id(id)
        sqlquery = f"SELECT levels_edge_list FROM data_address_schemas WHERE schema_id = '{schema_id}'"
        # conn = ResearchObjectHandler.pool.get_connection()
        conn = action.conn
        result = conn.execute(sqlquery).fetchone()

        # 5. If the schema is not None, convert the string to a list of types.
        str_schema = json.loads(result[0])
        schema = []
        for sch in str_schema:
            for idx, prefix in enumerate(sch):
                sch[idx] = ResearchObjectHandler._prefix_to_class(prefix)
            schema.append(sch)  

        return schema

    ### Dataset path methods

    def validate_dataset_path(self, path: str, action: Action, default: Any) -> None:
        """Validate the dataset path."""        
        if path == default:
            return
        if not os.path.exists(path):
            raise ValueError("Specified path is not a path or does not currently exist!")
        
    def load_dataset_path(self, action: Action) -> str:
        """Load the dataset path from the database in a computer-specific way."""
        return ResearchObjectHandler.get_user_computer_path(self, "dataset_path", action)
        
    ### Address Methods
        
    def validate_addresses(self, addresses: list, action: Action, default: Any) -> None:
        """Validate that the addresses are in the correct format."""
        if addresses == default:
            return
        self.validate_schema(self.schema, action, None)   

        try:
            graph = nx.MultiDiGraph()
            graph.add_edges_from(addresses)
        except nx.NetworkXError:
            raise ValueError("The addresses must be provided as an edge list!")
        
        if not nx.is_directed_acyclic_graph(graph):
            raise ValueError("The addresses must be a directed acyclic graph!")
        
        non_ro_id = [node for node in graph if not IDCreator(action.conn).is_ro_id(node)]
        if non_ro_id:
            raise ValueError("The addresses must only include ResearchObject ID's!")
                
        if not graph[self.id]:
            raise ValueError("The addresses must include the dataset ID!")
        
        vrs = [node for node in graph if node.startswith("VR")]
        if vrs:
            raise ValueError("The addresses must not include Variable ID's!")
        
        schema = self.schema
        schema_graph = nx.MultiDiGraph()
        schema_graph.add_edges_from(schema)
        for address_edge in addresses:
            cls0 = ResearchObjectHandler._prefix_to_class(address_edge[0])
            cls1 = ResearchObjectHandler._prefix_to_class(address_edge[1])
            if cls0 not in schema_graph.predecessors(cls1) or cls1 not in schema_graph.successors(cls0):
                raise ValueError("The addresses must match the schema!")
                
    def save_addresses(self, addresses: list, action: Action) -> list:
        """Save the addresses to the data_addresses table in the database."""        
        # 1. Get the schema_id for the current dataset_id that has not been overwritten by an Action.       
        dataset_id = self.id
        schema_id = self.get_current_schema_id(dataset_id)                
        for address_names in addresses:
            params = (address_names[0], address_names[1], schema_id, action.id)
            action.add_sql_query(self.id, "addresses_insert", params, group_name = "robj_complex_attr_insert")            
        self.__dict__["address_graph"] = self.addresses_to_graph(addresses, action)        

    def load_addresses(self, action: Action) -> list:
        """Load the addresses from the database."""
        pool = SQLiteConnectionPool()        
        schema_id = self.get_current_schema_id(self.id)
        conn = pool.get_connection()

        # 2. Get the addresses for the current schema_id.
        sqlquery = f"SELECT target_object_id, source_object_id FROM data_addresses WHERE schema_id = '{schema_id}'"
        addresses = conn.execute(sqlquery).fetchall()
        pool.return_connection(conn)

        # 3. Convert the addresses to a list of lists (from a list of tuples).
        addresses = [list(address) for address in addresses]                
        self.__dict__["address_graph"] = self.addresses_to_graph(addresses, action)

        return addresses

    def addresses_to_graph(self, addresses: list, action: Action) -> nx.MultiDiGraph:
        """Convert the addresses edge list to a MultiDiGraph."""
        G = nx.MultiDiGraph()
        # To avoid recursion, set the lines with Dataset manually so there is no self reference.
        address_copy = copy.deepcopy(addresses)
        for idx, address in enumerate(addresses):
            if address[0] == self.id: # Include the Dataset as the source node.
                cls1 = ResearchObjectHandler._prefix_to_class(address[1])
                G.add_edge(self, cls1(id = address[1], action = action))
                address_copy.remove(address)

        addresses = address_copy
        subclasses = DataObject.__subclasses__()
        cls_dict = {cls.prefix: cls for cls in subclasses}
        idcreator = IDCreator(action.conn)
        for address_edge in addresses:            
            cls0 = cls_dict[idcreator.get_prefix(address_edge[0])]
            cls1 = cls_dict[idcreator.get_prefix(address_edge[1])]
            G.add_edge(cls0(id = address_edge[0], action = action), cls1(id = address_edge[1], action = action))
        return G
    
if __name__=="__main__":
    pass