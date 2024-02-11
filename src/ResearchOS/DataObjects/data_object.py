"""The base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 
from typing import Any
from ResearchOS.research_object import ResearchObject
import json, os

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
        all_default_attrs = default_attrs | all_default_attrs
        super().__init__(all_default_attrs, **kwargs)

    def __setattr__(self, name: str, value: Any) -> None:
        """Set the value of an attribute in the DataObject and the database."""
        # TODO: HOW CAN I HANDLE VR VALUES BEING ADDED, MODIFIED, AND DELETED? 
        # In the format of self.vr = {vr_id: value}
        
        # 1. Get the ID's of all the VR's being added.
        vr_ids = []
        for vr_id in value:
            if vr_id not in self.vr:
                vr_ids.append(vr_id)
        

    
    def load(self) -> None:
        """Load data values from the database."""
        self.load_data_values()        

    def load_data_values(self) -> None:
        """Load data values from the database."""
        # 1. Identify which rows in the database are associated with this data object and have not been overwritten.
        sqlquery = f"SELECT dataobject_id, action_id, vr_id, scalar_value FROM data_values WHERE dataobject_id = {self.id}"
        conn = DBConnectionFactory.create_db_connection().conn
        cursor = conn.cursor()
        result = cursor.execute(sqlquery).fetchall()
        ordered_result = ResearchObjectHandler._get_time_ordered_result(result, action_col_num=1)
        latest_result = ResearchObjectHandler._get_most_recent_attrs(self, ordered_result)

        # "latest_result" is a list containing each of the attrs. There should be no repetition of attr names between elements of the list.
        vrs = {}
        for row in latest_result:
            vr_id = row[2]
            scalar_value = json.loads(row[3])
            if scalar_value is not None:
                vrs[vr_id] = scalar_value
                continue

            # TODO: correct this, this is a placeholder.
            path = root_data_path + os.sep + dataset_id + os.sep + vr_id + ".json"
            with open(path, "r") as f:
                # Is it json? Or a binary format?
                vrs[vr_id] = json.load(f)

        self.__dict__["vr"] = vrs