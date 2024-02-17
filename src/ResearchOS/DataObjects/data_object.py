"""The base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 
from typing import Any, TYPE_CHECKING
import json, os

from ResearchOS.action import Action
from ResearchOS.default_attrs import DefaultAttrs
from ResearchOS.research_object import ResearchObject
from ResearchOS.db_connection_factory import DBConnectionFactory
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator

if TYPE_CHECKING:
    from ResearchOS.variable import Variable

all_default_attrs = {}
# all_default_attrs["vr"] = {}

complex_attrs_list = []

# Root folder where all data is stored.
root_data_path = "data"

class DataObject(ResearchObject):
    """The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage."""    

    def __init__(self, default_attrs: dict, **kwargs) -> None:
        """Initialize the data object."""
        all_default_attrs_all = all_default_attrs | default_attrs # Class-specific default attributes take precedence.
        super().__init__(all_default_attrs_all, **kwargs)

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""
        # 1. Detect if the value is a VR, or if the name corresponds to a VR's name.
        ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)

    def __getattribute__(self, name: str) -> Any:
        """Get the value of an attribute. Only does any magic if the attribute exists already and is a VR."""        
        subclasses = DataObject.__subclasses__()
        vr_class = [x for x in subclasses if x.prefix == "VR"][0]
        try:
            value = super().__getattribute__(name) # Throw the default error.
        except AttributeError as e:
            raise e        
        if not isinstance(value, vr_class):
            return value
        
        ## Where the magic happens.
        # If the attribute is a VR, get the value from the database.
        conn = ResearchObjectHandler.pool.get_connection()
        schema_id = self.get_current_schema_id()
        sqlquery = f"SELECT scalar_value FROM data_values WHERE address_id = '{self.id}' AND schema_id = '{schema_id}' AND vr_id = '{value.id}'"
        json_value = conn.cursor().execute(sqlquery).fetchall()
        if len(value) < 1:
            raise ValueError("The VR does not exist in the database for this DataObject.")
        if len(value) > 1:
            raise ValueError("There are multiple VRs with the same ID in the database for this DataObject.")
        value = json.loads(json_value[0][0])
        if value is None:
            # Get the value from the file system.
            dataset_id = self.get_dataset_id(schema_id)
            levels = self.get_levels(schema_id, self.id)
            path = self.get_vr_file_path(value.id, dataset_id, levels)
            if os.path.exists(path):
                with open(path, "r") as f:
                    value = json.load(f)   
        ResearchObjectHandler.pool.return_connection(conn)
        return value


    def load(self) -> None:
        """Load the data object from the database."""
        pass
        # self.load_data_values()

    # def set_value(self, vr: "Variable", value: Any) -> None:
    #     """Set the value of a VR for a specific object."""
    #     address_id = self.id
    #     schema_id = self.get_current_schema_id()
    #     action = Action(name = "set VR value")
    #     self.add_vr_row(vr.id, value, schema_id, action)

    # def add_vr_row(self, vr_id: str, value: Any, schema_id: str, action: Action) -> None:
    #     """Add a VR to the DataObject. Also serves to modify existing objects."""
    #     json_value = json.dumps(None)
    #     is_scalar = False
    #     if isinstance(value, (int, float, bool, str, bytes)): # If is scalar.
    #         json_value = json.dumps(value)
    #         is_scalar = True
    #     sqlquery = f"INSERT INTO data_values (action_id, address_id, schema_id, vr_id, scalar_value) VALUES ({action.id}, {self.id}, {schema_id}, {vr_id}, {json_value})"
    #     action.add_sql_query(sqlquery)

    #     dataset_id = self.get_dataset_id(schema_id)

    #     # Save the data to the file system.
    #     # Get the list of levels for this address and dataset schema.
    #     if not is_scalar:
    #         self.save_value(value, vr_id, dataset_id, schema_id)

    # def save_value(self, value: Any, vr_id: str, dataset_id: str, schema_id: str) -> None:
    #     """Save data values to the file system."""
    #     # TODO: Change JSON to HDF5 or mat or some other binary format.
    #     levels = self.get_levels(schema_id, self.id)
    #     path = self.get_vr_file_path(vr_id, dataset_id, levels)
    #     with open(path, "w") as f:
    #         json.dump(value, f)

    # def load_data_values(self) -> None:
    #     """Load data values from the database."""        
        # 1. Get all of the latest address_id & vr_id combinations (that have not been overwritten) for the current schema for the current database.
        # Get the schema_id.
        # TODO: Put the schema_id into the data_values table.

    # def get_levels(self, schema_id: str, object_id: str = None) -> list:
    #     """Get the levels of the data object."""
    #     if object_id is not None:
    #         sqlquery = f"SELECT level0_id, level1_id, level2_id, level3_id, level4_id, level5_id, level6_id, level7_id, level8_id, level9_id FROM data_addresses WHERE address_id = '{object_id}' AND schema_id = '{schema_id}' LIMIT 1"
    #     else:
    #         sqlquery = f"SELECT level0_id, level1_id, level2_id, level3_id, level4_id, level5_id, level6_id, level7_id, level8_id, level9_id FROM data_addresses WHERE schema_id = '{schema_id}'"
    #     conn = DBConnectionFactory.create_db_connection().conn
    #     cursor = conn.cursor()
    #     levels = cursor.execute(sqlquery).fetchall()
    #     if len(levels) < 1:
    #         # raise ValueError("The address does not exist in the database.")
    #         return {}
        # if len(levels) > 1:
        #     raise ValueError("There are multiple addresses with the same ID in the database.")
        # Remove the None components from each row of the result.
        # trimmed_levels = []
        # for row in levels:
        #     trimmed_levels.append([x for x in row if x is not None])
        # levels = ResearchObjectHandler.list_to_dict(trimmed_levels)
        # levels_dict = {}
        # for row_num in range(len(levels)):
        #     for col_num in range(len(levels[row_num])):
        #         object_id = levels[row_num][col_num]
        #         if object_id is not None and object_id not in levels_dict:
        #             levels_dict[object_id] = {}
        # return levels_dict

    def get_vr_file_path(self, vr_id: str, dataset_id: str, levels: list) -> str:
        """Get the file path for a VR."""
        subfolder = ""
        for level in levels:
            if level is not None:
                subfolder += level + os.sep
        return root_data_path + os.sep + dataset_id + subfolder + vr_id + ".json"    

    def get_current_schema_id(self, dataset_id: str) -> str:
        conn = ResearchObjectHandler.pool.get_connection()
        sqlquery = f"SELECT action_id FROM data_address_schemas WHERE dataset_id = '{dataset_id}'"
        action_ids = conn.cursor().execute(sqlquery).fetchall()
        action_ids = ResearchObjectHandler._get_time_ordered_result(action_ids, action_col_num=0)
        action_id_schema = action_ids[0][0] if action_ids else None
        if action_id_schema is None and not self.schema:
            return # If the schema is empty and the addresses are empty, this is likely initialization so just return.

        sqlquery = f"SELECT schema_id FROM data_address_schemas WHERE dataset_id = '{dataset_id}' AND action_id = '{action_id_schema}'"
        schema_id = conn.execute(sqlquery).fetchone()
        schema_id = schema_id[0] if schema_id else None
        ResearchObjectHandler.pool.return_connection(conn)
        return schema_id    