from typing import Any
import json

from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.user import User
from ResearchOS.PipelineObjects.project import Project
from ResearchOS.DataObjects.dataset import Dataset
from ResearchOS.DataObjects.trial import Trial
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.db_connection_factory import DBConnectionFactory

all_default_attrs = {}
# all_default_attrs["hard_coded_value"] = None
all_default_attrs["level"] = Trial # Eventually needs to be a complex attr.
# all_default_attrs["value"] = None # Complex attr.

complex_attrs_list = []

class Variable(DataObject,  PipelineObject):
    """Variable class."""

    prefix: str = "VR"
    
    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        if "name" not in kwargs:
            kwargs["name"] = kwargs["id"] # "name" will always have a default value for Variables.
        super().__init__(all_default_attrs, **kwargs)

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""        
        ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)

    def load(self) -> None:
        """Load the variable-specific attributes from the database in an attribute-specific way."""
        pass
    
    ## Level methods

    def validate_level(self, level: type) -> None:
        """Check that the level is of a valid type."""
        if not isinstance(level, type):
            raise ValueError("Level must be a type.")
        if level not in DataObject.__subclasses__():
            raise ValueError("Level must be a DataObject.")
        # us = User(id = User.get_current_user_object_id())
        # us.validate_current_project_id(id = us.current_project_id)
        # pj = Project(id = us.current_project_id)
        # pj.validate_current_dataset_id(id = pj.current_dataset_id)
        # ds = Dataset(id = pj.current_dataset_id)
        # ds.validate_schema(schema = ds.schema)
        # if level not in ds.schema:
        #     raise ValueError("Level must be in the dataset schema.")
        
    def to_json_level(self, level: type) -> dict:
        """Return the level as a JSON object."""
        return json.dumps(level.prefix)

    def from_json_level(self, level: str) -> type:
        """Return the level as a JSON object."""
        level_str = json.loads(level)
        subclasses = DataObject.__subclasses__()
        return [cls for cls in subclasses if cls.prefix == level_str][0]
    
    # def load_value(self, address_id: str) -> None:
    #     """Load data values from the database OR file system for the specified address_id."""
    #     # 1. Identify which rows in the database are associated with this data object and have not been overwritten.
    #     schema_id = self.get_current_schema_id()
    #     sqlquery = f"SELECT address_id, schema_id, action_id, vr_id, scalar_value FROM data_values WHERE address_id = '{address_id}' AND schema_id = {schema_id}"
    #     conn = DBConnectionFactory.create_db_connection().conn
    #     cursor = conn.cursor()
    #     result = cursor.execute(sqlquery).fetchall()
    #     latest_result = ResearchObjectHandler._get_most_recent_attrs(self, result)

    #     if not latest_result:
    #         # raise ValueError("This object does not have an address_id in the database.")
    #         return
        
    #     schema_id = latest_result["schema_id"]
    #     dataset_id = self.get_dataset_id(schema_id)
    #     # Get the list of levels for this address and dataset schema.
    #     levels = self.get_levels(schema_id, address_id)

    #     # "latest_result" is a list containing each of the attrs. There should be no repetition of attr names between elements of the list.
    #     vrs = {}
    #     for row in latest_result:
    #         vr_id = row[3]
    #         scalar_value = json.loads(row[4])
    #         if scalar_value is not None:
    #             vrs[vr_id] = scalar_value
    #             continue

    #         # TODO: correct this, this is a placeholder.
    #         path = self.get_vr_file_path(vr_id, dataset_id, levels)
    #         with open(path, "r") as f:
    #             # Is it json? Or a binary format?
    #             vrs[vr_id] = json.load(f)

    #     self.__dict__["value"] = vrs
    
    # def save_value(self, value: Any, action: Action = None) -> None:
    #     """Save the value to the database."""
    #     # Set variable value.
    #     if action is None:
    #         action = Action(name = "set variable value")
    #     # 1. Get the address that the variable is pointing to.
            
    #     # 2. If scalar, set the value for the address in the data_values table.
    #     if isinstance(value, (int, float, str, bool, None)):
    #         is_scalar = True
    #         sqlquery = f"INSERT INTO data_values (action_id, address_id, scalar_value) VALUES ('{action.id}', '{self.address_id}', '{value}')"            
    #     else:
    #         is_scalar = False
    #         sqlquery = f"INSERT INTO data_values (action_id, address_id) VALUES ('{action.id}', '{self.address_id}')"
    #     action.add_sql_query(sqlquery)
    #     action.execute()
            
        # 3. If not scalar, save the value to the corresponding file in the file system.        
        # if not is_scalar:
        #     pass
        #     # 1. Get the file path from the address.

        #     # 2. Save it.        

    #################### Start Source objects ####################
    def get_source_object_ids(self, cls: type) -> list:
        """Return a list of all source objects for the Variable."""
        ids = self._get_all_source_object_ids(cls = cls)
        return [cls(id = id) for id in ids]
    
    ## NOTE: THERE ARE NO TARGET OBJECTS FOR VARIABLES