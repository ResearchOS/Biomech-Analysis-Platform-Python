from typing import Any, TYPE_CHECKING
import json, copy

import networkx as nx

# from ResearchOS.Graphs.data_graph import DataGraph
# from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.db_connection_factory import DBConnectionFactory

all_default_attrs = {}
all_default_attrs["schema"] = [] # Dict of empty dicts, where each key is the class and the value is a dict with subclass type(s).
all_default_attrs["dataset_path"] = None # str
all_default_attrs["addresses"] = [] # Dict of empty dicts, where each key is the ID of the object and the value is a dict with the subclasses' ID's.

complex_attrs_list = ["schema", "addresses"]

class Dataset(DataObject):
    """A dataset is one set of data.
    Class-specific Attributes:
    1. data path: The root folder location of the dataset.
    2. data schema: The schema of the dataset (specified as a list of classes)"""

    prefix: str = "DS"

    # def __init__(self, **kwargs):
    #     """Initialize the attributes that are required by ResearchOS.
    #     Other attributes can be added & modified later."""
    #     super().__init__(all_default_attrs, **kwargs)

    # def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
    #     """Set the attribute value. If the attribute value is not valid, an error is thrown."""
    #     ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)

    def load(self) -> None:
        """Load the dataset-specific attributes from the database in an attribute-specific way."""
        self.load_schema() # Load the dataset schema.
        self.load_addresses() # Load the dataset addresses.
        DataObject.load(self) # Load the attributes specific to it being a DataObject.

    ### Schema Methods
        
    def validate_schema(self, schema: list[list]) -> None:
        """Validate that the data schema follows the proper format.
        Must be a dict of dicts, where all keys are Python types matching a DataObject subclass, and the lowest levels are empty."""
        subclasses = DataObject.__subclasses__()
        vr = [x for x in subclasses if x.prefix == "VR"][0]                
            
        graph = nx.MultiDiGraph()
        try:
            graph.add_edges_from(schema)
        except nx.NetworkXError:
            raise ValueError("The schema must be provided as an edge list!")
        if not nx.is_directed_acyclic_graph(graph):
            raise ValueError("The schema must be a directed acyclic graph!")
        
        non_subclass = [node for node in graph if node not in subclasses]
        if non_subclass:
            raise ValueError("The schema must only include ResearchObject subclasses!")
        
        if Dataset not in graph:
            raise ValueError("The schema must include the Dataset class as a source node!")
        
        if vr in graph:
            raise ValueError("The schema must not include the Variable class as a target node!")
        
        # nodes_with_no_targets = [node for node, out_degree in graph.out_degree() if out_degree == 0]
        # nodes_with_a_source = [node for node, in_degree in graph.in_degree() if in_degree > 0]
        # if graph[Dataset] in nodes_with_no_targets or graph[Dataset] in nodes_with_a_source:
        #     raise ValueError("The schema must include the Dataset class as a source node and not a target node!")

    def save_schema(self, schema: list[list], action: Action) -> None:
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
        schema_id = IDCreator().create_action_id()
        sqlquery = f"INSERT INTO data_address_schemas (schema_id, levels_edge_list, dataset_id, action_id) VALUES ('{schema_id}', '{json_schema}', '{self.id}', '{action.id}')"
        action.add_sql_query(sqlquery)

    def load_schema(self) -> None:
        """Load the schema from the database and convert it via json."""
        # 1. Get the dataset ID
        id = self.id
        # 2. Get the most recent action ID for the dataset in the data_address_schemas table.
        schema_id = self.get_current_schema_id(id)
        sqlquery = f"SELECT levels_edge_list FROM data_address_schemas WHERE schema_id = '{schema_id}'"
        conn = ResearchObjectHandler.pool.get_connection()
        result = conn.execute(sqlquery).fetchone()

        # 5. If the schema is not None, convert the string to a list of types.
        str_schema = json.loads(result[0])
        schema = []
        for sch in str_schema:
            for idx, prefix in enumerate(sch):
                sch[idx] = ResearchObjectHandler._prefix_to_class(prefix)
            schema.append(sch)  

        # 6. Store the schema as an attribute of the dataset.
        self.__dict__["schema"] = schema

    ### Dataset path methods

    def validate_dataset_path(self, path: str) -> None:
        """Validate the dataset path."""
        import os
        if not os.path.exists(path):
            raise ValueError("Specified path is not a path or does not currently exist!")
        
    ### Address Methods
        
    def validate_addresses(self, addresses: list[list]) -> None:
        """Validate that the addresses are in the correct format."""
        self.validate_schema(self.schema)
        conn = DBConnectionFactory.create_db_connection().conn        

        try:
            graph = nx.MultiDiGraph()
            graph.add_edges_from(addresses)
        except nx.NetworkXError:
            raise ValueError("The addresses must be provided as an edge list!")
        
        if not nx.is_directed_acyclic_graph(graph):
            raise ValueError("The addresses must be a directed acyclic graph!")
        
        non_ro_id = [node for node in graph if not IDCreator().is_ro_id(node)]
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
                
    def save_addresses(self, addresses: list[list], action: Action) -> None:
        """Save the addresses to the data_addresses table in the database."""        
        # 1. Get the schema_id for the current dataset_id that has not been overwritten by an Action.       
        dataset_id = self.id
        schema_id = self.get_current_schema_id(dataset_id)
        for address_names in addresses:   
            sqlquery = f"INSERT INTO data_addresses (target_object_id, source_object_id, schema_id, action_id) VALUES ('{address_names[0]}', '{address_names[1]}', '{schema_id}', '{action.id}')"
            action.add_sql_query(sqlquery)

    def load_addresses(self) -> list[list]:
        """Load the addresses from the database."""
        conn = DBConnectionFactory.create_db_connection().conn
        schema_id = self.get_current_schema_id(self.id)

        # 2. Get the addresses for the current schema_id.
        sqlquery = f"SELECT target_object_id, source_object_id FROM data_addresses WHERE schema_id = '{schema_id}'"
        addresses = conn.execute(sqlquery).fetchall()

        # 3. Convert the addresses to a list of lists.
        addresses = [list(address) for address in addresses]

        G = nx.MultiDiGraph()
        # To avoid recursion, set the lines with Dataset manually so there is no self reference.
        address_copy = copy.deepcopy(addresses)
        if addresses:            
            for idx, address in enumerate(addresses):
                if address[0] == self.id:
                    cls1 = ResearchObjectHandler._prefix_to_class(address[1])
                    G.add_edge(self, cls1(id = address[1]))
                    address_copy.remove(address)

        addresses = address_copy
        for address_edge in addresses:
            cls0 = ResearchObjectHandler._prefix_to_class(address_edge[0])
            cls1 = ResearchObjectHandler._prefix_to_class(address_edge[1])
            G.add_edge(cls0(id = address_edge[0]), cls1(id = address_edge[1]))

        self.__dict__["addresses"] = addresses
        self.__dict__["address_graph"] = G
    
if __name__=="__main__":
    pass