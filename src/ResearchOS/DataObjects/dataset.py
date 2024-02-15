from typing import Any, TYPE_CHECKING
import json, copy

from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.db_connection_factory import DBConnectionFactory

all_default_attrs = {}
all_default_attrs["schema"] = {} # Dict of empty dicts, where each key is the class and the value is a dict with subclass type(s).
all_default_attrs["dataset_path"] = None # str
all_default_attrs["addresses"] = {} # Dict of empty dicts, where each key is the ID of the object and the value is a dict with the subclasses' ID's.

complex_attrs_list = ["schema", "addresses"]

def class_to_prefix(schema: dict) -> dict:
    """Convert the dict with a class key to a dict with a str prefix key."""
    new_schema = {}
    for key, value in schema.items():
        new_schema[key.prefix] = class_to_prefix(value)                
    return new_schema

def prefix_to_class(schema: dict) -> dict:
    """Convert the dict with a str prefix key to a dict with a class key."""
    new_schema = {}
    for key, value in schema.items():
        new_schema[ResearchObjectHandler._prefix_to_class(key)] = prefix_to_class(value)                
    return new_schema

class Dataset(DataObject):
    """A dataset is one set of data.
    Class-specific Attributes:
    1. data path: The root folder location of the dataset.
    2. data schema: The schema of the dataset (specified as a list of classes)"""

    prefix: str = "DS"

    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""        
        is_new = False
        if not ResearchObjectHandler.object_exists(kwargs.get("id")):
            is_new = True
        super().__init__(all_default_attrs, **kwargs)   
        if is_new:     
            schema = {
                Dataset: {}
            }
            self.schema = schema

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""
        ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)

    def load(self) -> None:
        """Load the dataset-specific attributes from the database in an attribute-specific way."""
        self.load_schema() # Load the dataset schema.
        self.load_addresses() # Load the dataset addresses.
        DataObject.load(self) # Load the attributes specific to it being a DataObject.

    ### Schema Methods
        
    def validate_schema(self, schema: dict[dict]) -> None:
        """Validate that the data schema follows the proper format.
        Must be a dict of dicts, where all keys are Python types matching a DataObject subclass, and the lowest levels are empty."""
        subclasses = DataObject.__subclasses__()
        # Check if the object is a dictionary
        if not isinstance(schema, dict):
            raise ValueError("The schema must be a dictionary!")
        
        for key, value in schema.items():
            # Check if the key is an instance of a DataObject subclass Python type
            if not key in subclasses:
                raise ValueError("The key must be an instance of a DataObject subclass Python type!")
            
            self.validate_schema(value)

    def save_schema(self, schema: list, action: Action) -> None:
        """Save the schema to the database."""
        # 1. Convert the list of types to a list of str.
        str_schema = class_to_prefix(schema)

        # 2. Convert the list of str to a json string.
        json_schema = json.dumps(str_schema)

        # 3. Save the schema to the database.
        conn = DBConnectionFactory.create_db_connection().conn
        schema_id = IDCreator(conn).create_action_id()
        sqlquery = f"INSERT INTO data_address_schemas (schema_id, levels_edge_list, dataset_id, action_id) VALUES ('{schema_id}', '{json_schema}', '{self.id}', '{action.id}')"
        action.add_sql_query(sqlquery)

    def load_schema(self) -> None:
        """Load the schema from the database and convert it via json."""
        prefix_schema = all_default_attrs["schema"] # Initialize the schema
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
        schema = prefix_to_class(prefix_schema)    

        # 6. Store the schema as an attribute of the dataset.
        self.__dict__["schema"] = schema

    ### Dataset path methods

    def validate_dataset_path(self, path: str) -> None:
        """Validate the dataset path."""
        import os
        if not os.path.exists(path):
            raise ValueError("Specified path is not a path or does not currently exist!")
        
    ### Address Methods
        
    def validate_addresses(self, addresses: dict, count: int = 0) -> None:
        """Validate that the addresses are in the correct format."""
        conn = DBConnectionFactory.create_db_connection().conn
        if count==0: # Only do this once.
            self.validate_schema(self.schema) # Ensure that the schema is valid before doing the addresses.
        if count > 9:
            raise ValueError("Address dict must be of depth 10 or less!")
        
        if not isinstance(addresses, dict):
            raise ValueError("Addresses must be provided as a dict!")
        
        for key, value in addresses.items():
            if not isinstance(key, str):
                raise ValueError("addresses must be str!")
            if not IDCreator(conn).is_ro_id(key):
                raise ValueError("Addresses must have the proper prefixes to be considered valid!")
            if not ResearchObjectHandler.object_exists(key):
                raise ValueError("The object at this address does not exist in the database!")
            self.validate_addresses(value, count + 1)
                
    def save_addresses(self, addresses: dict, action: Action) -> None:
        """Save the addresses to the data_addresses table in the database."""        
        # 1. Get the schema_id for the current dataset_id that has not been overwritten by an Action.        
        dataset_id = self.id
        schema_id = self.get_current_schema_id(dataset_id)

        # 2. Put the addresses into the data_addresses table.
        def save_addresses_recurse(address_dict: dict, schema_id: str, action: Action, level_num: int = 0, prev_addresses_list: list = []) -> None:
            """Recursively save the addresses to the database."""            
            for address in address_dict:
                # 1. Save the current address.
                full_address_list = copy.deepcopy(prev_addresses_list)
                full_address_list.append(address)
                level_names = ""
                for idx, level in enumerate(full_address_list):
                    level_names += f"level{idx}_id, "
                level_names = level_names[:-2] # Remove final comma and space.
                sqlquery = f"INSERT INTO data_addresses (schema_id, action_id, address_id, {level_names}) VALUES ('{schema_id}', '{action.id}', '{address}'"
                for level in full_address_list:
                    sqlquery += f", '{level}'"
                sqlquery += ")"
                action.add_sql_query(sqlquery)
                # 2. Recurse.
                save_addresses_recurse(address_dict[address], schema_id, action, level_num + 1, full_address_list)

        save_addresses_recurse(addresses, schema_id, action)
            
            # level_names = ""
            # for level in address:
            #     level_names += f"level{address.index(level)}_id, "
            # level_names = level_names[:-2] # Remove final comma and space.
            
            # for level in address:
            #     sqlquery += f", '{level}'"
            # sqlquery += ")"
        #     for address_id, value in addresses.items():
        #         sqlquery = f"INSERT INTO data_addresses (schema_id, action_id, address_id, level{level_num}_id) VALUES ('{schema_id}', '{action.id}', '{address_id}', '{address_id}')"
        #         action.add_sql_query(sqlquery)
        #         save_addresses_recurse(value, schema_id, action)

        # for address in addresses:
        #     # address_id = IDCreator(conn).create_action_id()
        #     level_names = ""
        #     for level in address:
        #         level_names += f"level{address.index(level)}_id, "
        #     level_names = level_names[:-2] # Remove final comma and space.
        #     sqlquery = f"INSERT INTO data_addresses (schema_id, action_id, address_id, {level_names}) VALUES ('{schema_id}', '{action.id}', '{address_id}'"
        #     for level in address:
        #         sqlquery += f", '{level}'"
        #     sqlquery += ")"
        #     action.add_sql_query(sqlquery)

    def load_addresses(self) -> list[list]:
        """Load the addresses from the database."""
        # conn = DBConnectionFactory.create_db_connection().conn

        schema_id = self.get_current_schema_id(self.id)

        # 2. Get the addresses for the current schema_id.
        # sqlquery = f"SELECT action_id FROM data_addresses WHERE schema_id = '{schema_id}'"
        # action_ids = conn.execute(sqlquery).fetchall()
        # action_ids = ResearchObjectHandler._get_time_ordered_result(action_ids, action_col_num=0)
        # action_id = action_ids[0][0] if action_ids else None

        levels = self.get_levels(schema_id)
        self.__dict__["addresses"] = levels

    def load_address_objects(self, address_objects: dict = {}, address_dict: dict = {}) -> list:
        """Using the self.addresses property, load the addressed objects from the database."""
        dataobject_subclasses = DataObject.__subclasses__()
        if not address_dict:
            address_dict = self.addresses
        for address in address_dict:
            for cls in dataobject_subclasses:
                if not address.startswith(cls.prefix):
                    continue                
                break
            cls_object = cls(id = address)
            if cls_object in address_objects:
                continue
            address_objects[cls_object] = {}
            self.load_address_objects(address_objects = address_objects[cls_object], address_dict = address_dict[address])
            
        return address_objects
    
if __name__=="__main__":
    pass