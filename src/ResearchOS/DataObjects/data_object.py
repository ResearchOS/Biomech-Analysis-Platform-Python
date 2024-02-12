"""The base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 
from typing import Any
import json, os

from ResearchOS.action import Action
from ResearchOS.research_object import ResearchObject
from ResearchOS.db_connection_factory import DBConnectionFactory
from ResearchOS.research_object_handler import ResearchObjectHandler

all_default_attrs = {}
all_default_attrs["vr"] = None

# Root folder where all data is stored.
root_data_path = "data" 

class DataObject(ResearchObject):
    """The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage."""    

    def __init__(self, default_attrs: dict, **kwargs) -> None:
        """Initialize the data object."""
        all_default_attrs_all = default_attrs | all_default_attrs
        super().__init__(all_default_attrs_all, **kwargs)

    def __setattr__(self, name: str, value: Any) -> None:
        """Set the value of an attribute in the DataObject and the database."""
        # TODO: HOW CAN I HANDLE VR VALUES BEING ADDED, MODIFIED, AND DELETED?
        # In the format of self.vr = {vr_id: value}

        # Addition: There are new fields.
        # Deletion: There are fields that are no longer present.
        # Modification: The fields have changed.
        # NOTE: All/any combination of these three operations could happen at once.
        

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

        sqlquery = f"SELECT action_id, schema_id FROM data_addresses WHERE address_id = {self.id}"
        conn = DBConnectionFactory.create_db_connection().conn
        cursor = conn.cursor()
        result = cursor.execute(sqlquery).fetchall()
        ordered_result = ResearchObjectHandler._get_time_ordered_result(result, action_col_num=0)
        latest_result = ResearchObjectHandler._get_most_recent_attrs(self, ordered_result)
        schema_id = latest_result[0][1]

        action = Action(name = "vr changed")
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
        if isinstance(value, (int, float, bool, str, bytes)): # If is scalar.
            json_value = json.dumps(value)
        sqlquery = f"INSERT INTO data_values (action_id, address_id, schema_id, vr_id, scalar_value) VALUES ({action.id}, {self.id}, {schema_id}, {vr_id}, {json_value})"
        action.add_sql_query(sqlquery)

        # Save the data to the file system.
        # Get the list of levels for this address and dataset schema.
        self.save_data_values(value, levels)

    def delete_vr(self, vr_id: str) -> None:
        """Delete a VR from the DataObject."""
        pass

    def save_data_values(self, value: Any, levels: list) -> None:
        """Save data values to the file system."""
        pass

    
    def load(self) -> None:
        """Load data values from the database."""
        self.load_data_values()        

    def load_data_values(self) -> None:
        """Load data values from the database OR file system."""
        # 1. Identify which rows in the database are associated with this data object and have not been overwritten.
        sqlquery = f"SELECT address_id, schema_id, action_id, vr_id, scalar_value FROM data_values WHERE address_id = {self.id}"
        conn = DBConnectionFactory.create_db_connection().conn
        cursor = conn.cursor()
        result = cursor.execute(sqlquery).fetchall()
        ordered_result = ResearchObjectHandler._get_time_ordered_result(result, action_col_num=1)
        latest_result = ResearchObjectHandler._get_most_recent_attrs(self, ordered_result)

        schema_id = latest_result[0][1]

        # Get the dataset_id from data_address_schemas table.
        sqlquery = f"SELECT dataset_id FROM data_address_schemas WHERE schema_id = {schema_id} LIMIT 1"
        dataset_id = cursor.execute(sqlquery).fetchone()

        # Get the list of levels for this address and dataset schema.
        sqlquery = f"SELECT level0, level1, level2, level3, level4, level5, level6, level7, level8, level9 FROM data_addresses WHERE address_id = {self.id} AND schema_id = {schema_id} LIMIT 1"
        levels = cursor.execute(sqlquery).fetchall()
        if len(levels) < 1:
            raise ValueError("The address does not exist in the database.")
        if len(levels) > 1:
            raise ValueError("There are multiple addresses with the same ID in the database.")
        levels = levels[0]

        # "latest_result" is a list containing each of the attrs. There should be no repetition of attr names between elements of the list.
        vrs = {}
        for row in latest_result:
            vr_id = row[3]
            scalar_value = json.loads(row[4])
            if scalar_value is not None:
                vrs[vr_id] = scalar_value
                continue

            # TODO: correct this, this is a placeholder.
            subfolder = ""
            for level in levels:
                if level is not None:
                    subfolder += level + os.sep
            path = root_data_path + os.sep + dataset_id + subfolder + vr_id + ".json"
            with open(path, "r") as f:
                # Is it json? Or a binary format?
                vrs[vr_id] = json.load(f)

        self.__dict__["vr"] = vrs