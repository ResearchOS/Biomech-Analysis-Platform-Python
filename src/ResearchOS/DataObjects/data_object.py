"""The base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 
from typing import Any
import json, os

from ResearchOS.action import Action
from ResearchOS.research_object import ResearchObject
from ResearchOS.db_connection_factory import DBConnectionFactory
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator

all_default_attrs = {}
# all_default_attrs["vr"] = {}

# Root folder where all data is stored.
root_data_path = "data"

class DataObject(ResearchObject):
    """The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage."""    

    def __init__(self, default_attrs: dict, **kwargs) -> None:
        """Initialize the data object."""
        all_default_attrs_all = all_default_attrs | default_attrs # Class-specific default attributes take precedence.
        super().__init__(all_default_attrs_all, **kwargs)

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the value of an attribute in the DataObject and the database."""
        # TODO: HOW CAN I HANDLE VR VALUES BEING ADDED, MODIFIED, AND DELETED?
        # In the format of self.vr = {vr_id: value}

        # Addition: There are new fields.
        # Deletion: There are fields that are no longer present.
        # Modification: The fields have changed.
        # NOTE: All/any combination of these three operations could happen at once.

        if action is None:
            action = Action(name = "vr changed")

        conn = DBConnectionFactory.create_db_connection().conn
        if validate and not all([IDCreator(conn).is_ro_id(vr_id) for vr_id in value]):
            raise ValueError("The keys of the dictionary must be valid VR ID's.")

        # 1. Get the ID's of all the VR's being added.
        new_vr_ids = []
        for vr_id in value:
            if vr_id not in self.vr:
                new_vr_ids.append(vr_id)

        # 2. Get the ID's of all the VR's being modified.
        modified_vr_ids = []
        for vr_id in value:
            if vr_id in self.vr and self.vr[vr_id] != value[vr_id]:
                modified_vr_ids.append(vr_id)

        sqlquery = f"SELECT action_id, schema_id FROM data_addresses WHERE address_id = '{self.id}'"
        cursor = conn.cursor()
        result = cursor.execute(sqlquery).fetchall()
        ordered_result = ResearchObjectHandler._get_time_ordered_result(result, action_col_num=0)
        latest_result = ResearchObjectHandler._get_most_recent_attrs(self, ordered_result)
        if "schema_id" not in latest_result:
            # raise ValueError("This object does not have an address_id in the database.")
            return
        
        schema_id = latest_result["schema_id"]
        
        new_row_vr_ids = new_vr_ids + modified_vr_ids
        for vr_id in new_row_vr_ids:
            self.add_vr_row(vr_id, value[vr_id], schema_id, action)

        action.execute()

        # 3. Get the ID's of all the VR's being deleted.
        deleted_vr_ids = []
        for vr_id in self.vr:
            if vr_id not in value:
                deleted_vr_ids.append(vr_id)        

    def add_vr_row(self, vr_id: str, value: Any, schema_id: str, action: Action) -> None:
        """Add a VR to the DataObject. Also serves to modify existing objects."""
        json_value = json.dumps(None)
        is_scalar = False
        if isinstance(value, (int, float, bool, str, bytes)): # If is scalar.
            json_value = json.dumps(value)
            is_scalar = True
        sqlquery = f"INSERT INTO data_values (action_id, address_id, schema_id, vr_id, scalar_value) VALUES ({action.id}, {self.id}, {schema_id}, {vr_id}, {json_value})"
        action.add_sql_query(sqlquery)

        dataset_id = self.get_dataset_id(schema_id)

        # Save the data to the file system.
        # Get the list of levels for this address and dataset schema.
        if not is_scalar:
            self.save_data_values(value, vr_id, dataset_id, schema_id)

    def delete_vr(self, vr_id: str) -> None:
        """Delete a VR from the DataObject."""
        pass

    def save_data_values(self, value: Any, vr_id: str, dataset_id: str, schema_id: str) -> None:
        """Save data values to the file system."""
        levels = self.get_levels(schema_id)
        path = self.get_vr_file_path(vr_id, dataset_id, levels)
        with open(path, "w") as f:
            json.dump(value, f)

    def load(self) -> None:
        """Load data values from the database."""        
        pass    

    def get_levels(self, schema_id: str, object_id: str = None) -> list:
        """Get the levels of the data object."""
        if object_id is not None:
            sqlquery = f"SELECT level0_id, level1_id, level2_id, level3_id, level4_id, level5_id, level6_id, level7_id, level8_id, level9_id FROM data_addresses WHERE address_id = '{object_id}' AND schema_id = '{schema_id}' LIMIT 1"
        else:
            sqlquery = f"SELECT level0_id, level1_id, level2_id, level3_id, level4_id, level5_id, level6_id, level7_id, level8_id, level9_id FROM data_addresses WHERE schema_id = '{schema_id}'"
        conn = DBConnectionFactory.create_db_connection().conn
        cursor = conn.cursor()
        levels = cursor.execute(sqlquery).fetchall()
        if len(levels) < 1:
            # raise ValueError("The address does not exist in the database.")
            return {}
        # if len(levels) > 1:
        #     raise ValueError("There are multiple addresses with the same ID in the database.")
        # Remove the None components from each row of the result.
        trimmed_levels = []
        for row in levels:
            trimmed_levels.append([x for x in row if x is not None])
        levels = ResearchObjectHandler.list_to_dict(trimmed_levels)
        levels_dict = {}
        for row_num in range(len(levels)):
            for col_num in range(10):
                object_id = levels[row_num][col_num]
                if object_id is not None and object_id not in levels_dict:
                    levels_dict[object_id] = {}
        return levels_dict
                

    def get_vr_file_path(self, vr_id: str, dataset_id: str, levels: list) -> str:
        """Get the file path for a VR."""
        subfolder = ""
        for level in levels:
            if level is not None:
                subfolder += level + os.sep
        return root_data_path + os.sep + dataset_id + subfolder + vr_id + ".json"
    
    def get_dataset_id(self, schema_id: str) -> str:
        """Get the current dataset ID."""
        sqlquery = f"SELECT dataset_id FROM dataset_schemas WHERE schema_id = {schema_id} LIMIT 1"
        conn = DBConnectionFactory.create_db_connection().conn
        cursor = conn.cursor()
        dataset_id = cursor.execute(sqlquery).fetchall()
        if len(dataset_id) < 1:
            raise ValueError("The schema does not exist in the database.")
        if len(dataset_id) > 1:
            raise ValueError("There are multiple schemas with the same ID in the database.")
        return dataset_id[0][0]
    
    def get_address(self, attr_name: str) -> list:
        """Get the address for a specific attribute."""
        conn = DBConnectionFactory.create_db_connection().conn
        schema_id = self.get_schema_id()
        sqlquery = f"SELECT {attr_name} FROM data_addresses WHERE schema_id = '{schema_id}'"
        result = conn.execute(sqlquery).fetchone()
        return result
    
    def get_current_schema_id(self, dataset_id: str) -> str:
        conn = DBConnectionFactory.create_db_connection().conn
        sqlquery = f"SELECT action_id FROM data_address_schemas WHERE dataset_id = '{dataset_id}'"
        action_ids = conn.cursor().execute(sqlquery).fetchall()
        action_ids = ResearchObjectHandler._get_time_ordered_result(action_ids, action_col_num=0)
        action_id_schema = action_ids[0][0] if action_ids else None
        if action_id_schema is None and not self.schema:
            return # If the schema is empty and the addresses are empty, this is likely initialization so just return.

        sqlquery = f"SELECT schema_id FROM data_address_schemas WHERE dataset_id = '{dataset_id}' AND action_id = '{action_id_schema}'"
        schema_id = conn.execute(sqlquery).fetchone()
        schema_id = schema_id[0] if schema_id else None
        return schema_id