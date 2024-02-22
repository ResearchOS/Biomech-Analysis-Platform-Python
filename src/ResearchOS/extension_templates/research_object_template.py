from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.action import Action

from typing import Any
import json

# This variable must always be present.
all_default_attrs = {}
all_default_attrs["attr1"] = None

# This variable must always be present.
complex_attrs_list = []

class Template(PipelineObject, DataObject):

    prefix = "TP"

    def validate_attr1(self, attr1: type) -> None:
        """Validate the value of attr1. If one of the checks for validity fail, raise a ValueError (or any error besides AttributeError).
        The string "attr1" (after the underscore) must match the attribute name exactly.
        This method is only called during __setattr__, while the attribute is being set."""        
        raise NotImplementedError

    def from_json_attr1(self, json_value: str) -> Any:
        """Translate the value of attr1 from a JSON-friendly format.
        "attr1" must match the attribute name exactly.
        This method is called when loading an attribute from the research_objects_attributes table.
        This method should transform the JSON string into the appropriate type via a custom mechanism, and then return the value."""
        val = json.loads(json_value)
        raise NotImplementedError

    def to_json_attr1(self, attr1: Any) -> str:
        """Translate the value of attr1 into a JSON-friendly format.
        "attr1" must match the attribute name exactly.
        This method is called when storing an attribute in the research_objects_attributes table.
        This method should transform attr1 to be JSON serializable, and then return the JSON string."""
        json_value = json.dumps(self.attr1)
        raise NotImplementedError

    def save_attr1(self, attr1: type, action: Action) -> None:
        """Custom method to store the value of attr1. "attr1" must match the attribute name exactly.
        This method is called when storing an attribute in any table besides simple_attributes.
        The output of this method should be action.add_sql_query(sqlquery) to add to the list of sql_queries to be executed."""        
        sqlquery = ""
        action.add_sql_query(sqlquery)
    
    def load_attr1(self) -> Any:
        """Custom method to load the value of attr1. "attr1" must match the attribute name exactly.
        This method is called when loading an attribute from any table besides simple_attributes."""
        raise NotImplementedError
        
    