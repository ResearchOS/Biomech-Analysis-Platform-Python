from typing import Any
import json

import ResearchOS as ros
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.db_connection_factory import DBConnectionFactory

all_default_attrs = {}
all_default_attrs["schema"]  = []
all_default_attrs["dataset_path"] = None
all_default_attrs["addresses"] = []

complex_attrs_list = ["schema", "addresses"]

class Dataset(ros.DataObject):
    """A dataset is one set of data.
    Class-specific Attributes:
    1. data path: The root folder location of the dataset.
    2. data schema: The schema of the dataset (specified as a list of classes)"""

    prefix: str = "DS"
    _current_source_type_prefix = "PJ"
    _source_type_prefix = "PJ"

    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        super().__init__(all_default_attrs, **kwargs)

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""
        if name == "vr":
            ros.DataObject.__setattr__(self, name, value, action, validate)
        else:
            ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)

    def load(self) -> None:
        """Load the dataset-specific attributes from the database in an attribute-specific way."""
        self.load_schema() # Load the dataset schema.
        self.load_addresses() # Load the dataset addresses.
        ros.DataObject.load(self) # Load the attributes specific to it being a DataObject.

    ### Schema Methods
        
    def validate_schema(self, schema: list) -> None:
        """Validate that the data schema follows the proper format.
        Must be an edge list [source, target], which is a list of lists (of length 2)."""
        # TODO: Check that every element is unique, no repeats.
        if not isinstance(schema, list):
            raise ValueError("Schema must be provided as a list!")
        # if len(schema) == 0:
        #     return # They're resetting the schema.
        for idx, _ in enumerate(schema):
            if not isinstance(_, list):
                raise ValueError("Schema must be provided as a list of lists!")
            if len(_) != 2:
                raise ValueError("Schema must be provided as a list of lists of length 2!")
            if not isinstance(_[0], type) or not isinstance(_[1], type):
                raise ValueError("Schema must be provided as a list of lists of ResearchObject types!")
            if isinstance(_[0], ros.Variable) or isinstance(_[1], ros.Variable):
                raise ValueError("Do not include the Variable object in the schema! It is implicitly assumed to be the last element in the list")                
            if idx == 0 and _[0] != Dataset:
                raise ValueError("Dataset must be the first element in the first list as the origin/source node of the schema.")

    def save_schema(self, schema: list, action: Action) -> None:
        """Save the schema to the database."""
        # 1. Convert the list of types to a list of str.
        str_schema = []
        for edge_list in schema:
                str_schema.append([edge_list[0].prefix, edge_list[1].prefix])

        # 2. Convert the list of str to a json string.
        json_schema = json.dumps(str_schema)

        # 3. Save the schema to the database.
        conn = DBConnectionFactory.create_db_connection().conn
        schema_id = IDCreator(conn).create_action_id()
        sqlquery = f"INSERT INTO data_address_schemas (schema_id, levels_edge_list, dataset_id, action_id) VALUES ('{schema_id}', '{json_schema}', '{self.id}', '{action.id}')"
        action.add_sql_query(sqlquery)

        # 4. Store the dataset ID to the data_addresses table.
        address_id = IDCreator(conn).create_action_id()
        sqlquery = f"INSERT INTO data_addresses (action_id, schema_id, address_id) VALUES ('{action.id}', '{schema_id}', '{address_id}')"
        action.add_sql_query(sqlquery)   

    def load_schema(self) -> None:
        """Load the schema from the database and convert it via json."""
        prefix_schema = [] # Initialize the schema as an empty list.
        # 1. Get the dataset ID
        id = self.id
        # 2. Get the most recent action ID for the dataset in the data_address_schemas table.
        sqlquery = f"SELECT action_id FROM data_address_schemas WHERE dataset_id = '{id}'"
        conn = DBConnectionFactory.create_db_connection().conn
        result = conn.execute(sqlquery).fetchall()
        # result = [_[0] for _ in result]
        ordered_result = ResearchObjectHandler._get_time_ordered_result(result, action_col_num = 0)
        if len(ordered_result) > 0:
            most_recent_action_id = ordered_result[0][0]
            # 3. Get the schema from the levels_ordered column in the data_address_schemas table.
            sqlquery = f"SELECT levels_edge_list FROM data_address_schemas WHERE action_id = '{most_recent_action_id}'"
            result = conn.execute(sqlquery).fetchall()
            prefix_schema = json.loads(result[0][0])

        # 5. If the schema is not None, convert the string to a list of types.
        schema = []
        for edge_list in prefix_schema:
            schema.append([
                ResearchObjectHandler._prefix_to_class(self, edge_list[0]), ResearchObjectHandler._prefix_to_class(self, edge_list[1])
                ])

        # 6. Store the schema as an attribute of the dataset.
        self.__dict__["schema"] = schema

    ### Dataset path methods

    def validate_dataset_path(self, path: str) -> None:
        """Validate the dataset path."""
        import os
        if not os.path.exists(path):
            raise ValueError("Specified path is not a path or does not currently exist!")
        
    ### Address Methods
        
    def validate_addresses(self, addresses: list) -> None:
        """Validate that the addresses are in the correct format."""
        self.validate_schema(self.schema) # Ensure that the schema is valid before doing the addresses.
        conn = DBConnectionFactory.create_db_connection().conn
        if not isinstance(addresses, list):
            raise ValueError("Addresses must be provided as a list!")
        
        for address in addresses:
            if not isinstance(address, list):
                raise ValueError("Addresses must be provided as a list of lists!")            
            if len(address) > 10:
                raise ValueError("Addresses must be provided as a list of lists of length 10 or less!")
            for level in address:
                if not isinstance(level, str):
                    raise ValueError("Addresses must be provided as a list of lists of strings!")
                if not IDCreator(conn).is_ro_id(level):
                    raise ValueError("Addresses must be provided as a list of lists of ResearchObject IDs!")
            # Ensure that the addresses are of the proper type according to the schema.
            if len(address) == 1:
                if not address[0].startswith(self.prefix):
                    raise ValueError("This address be a dataset ID!")
                continue
            for idx, level in enumerate(address[1:]):                
                follows_schema = False
                for _ in self.schema:
                    if address[idx].startswith(_[0].prefix) and address[idx+1].startswith( _[1].prefix):
                        follows_schema = True
                        break       
                if not follows_schema:
                    raise ValueError("Addresses must follow the schema of the dataset!")
                
    def save_addresses(self, addresses: list, action: Action) -> None:
        """Save the addresses to the data_addresses table in the database."""
        conn = DBConnectionFactory.create_db_connection().conn

        # 1. Get the schema_id for the current dataset_id that has not been overwritten by an Action.        
        dataset_id = self.id
        sqlquery = f"SELECT action_id FROM data_address_schemas WHERE dataset_id = '{dataset_id}'"
        action_ids = conn.cursor().execute(sqlquery).fetchall()
        action_ids = ResearchObjectHandler._get_time_ordered_result(action_ids, action_col_num=0)
        action_id_schema = action_ids[0][0] if action_ids else None
        if action_id_schema is None and self.schema == []:
            return # If the schema is empty and the addresses are empty, this is likely initialization so just return.

        sqlquery = f"SELECT schema_id FROM data_address_schemas WHERE dataset_id = '{dataset_id}' AND action_id = '{action_id_schema}'"
        schema_id = conn.execute(sqlquery).fetchone()
        schema_id = schema_id[0] if schema_id else None

        # 2. Put the addresses into the data_addresses table.
        for address in addresses:
            address_id = IDCreator(conn).create_action_id()
            level_names = ""
            for level in address:
                level_names += f"level{address.index(level)}_id, "
            level_names = level_names[:-2] # Remove final comma and space.
            sqlquery = f"INSERT INTO data_addresses (schema_id, action_id, address_id, {level_names}) VALUES ('{schema_id}', '{action.id}', '{address_id}'"
            for level in address:
                sqlquery += f", '{level}'"
            sqlquery += ")"
            action.add_sql_query(sqlquery)

    def load_addresses(self) -> list:
        """Load the addresses from the database."""
        conn = DBConnectionFactory.create_db_connection().conn

        # 1. Get the schema_id for the current dataset_id that has not been overwritten by an Action.        
        sqlquery = f"SELECT action_id FROM data_address_schemas WHERE schema_id = '{self.id}'"
        action_ids = conn.execute(sqlquery).fetchall()
        action_ids = ResearchObjectHandler._get_time_ordered_result(action_ids, action_col_num=0)
        action_id = action_ids[0] if action_ids else None

        sqlquery = f"SELECT schema_id FROM data_address_schemas WHERE dataset_id = '{self.id}' AND action_id = '{action_id}'"
        schema_id = conn.execute(sqlquery).fetchone()
        schema_id = schema_id[0] if schema_id else None

        # 2. Get the addresses for the current schema_id.
        sqlquery = f"SELECT action_id FROM data_addresses WHERE schema_id = '{schema_id}'"
        action_ids = conn.execute(sqlquery).fetchall()
        action_ids = ResearchObjectHandler._get_time_ordered_result(action_ids, action_col_num=0)
        action_id = action_ids[0] if action_ids else None

        sqlquery = f"""SELECT level0_id, level1_id, level2_id, level3_id, level4_id, level5_id, level6_id, level7_id, level8_id, level9_id FROM data_addresses WHERE schema_id = '{schema_id}' AND action_id = '{action_id}'"""
        result = conn.execute(sqlquery).fetchall()
        # Remove the None components from each row of the result.
        trimmed_result = []
        for row in result:
            trimmed_result.append([x for x in row if x is not None])

        self.__dict__["addresses"] = trimmed_result
    
if __name__=="__main__":
    pass