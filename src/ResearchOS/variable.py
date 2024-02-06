from ResearchOS import DataObject
from ResearchOS import PipelineObject
import ResearchOS as ros

from abc import abstractmethod

default_instance_attrs = {}
default_instance_attrs["hard_coded_value"] = None
default_instance_attrs["level"] = None

class Variable(DataObject, PipelineObject):
    """Variable class."""

    prefix: str = "VR"           

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Variable)
    
    #################### Start class-specific attributes ###################
    def validate_level(self, level: type) -> None:
        """Check that the level is of a valid type."""
        if not isinstance(level, type):
            raise ValueError("Level must be a type.")
        if not isinstance(level, DataObject):
            raise ValueError("Level must be a DataObject.")
        us = ros.User(id = ros.User.get_current_user_object_id())
        us.validate_current_project_id(id = us.current_project_id)
        pj = ros.Project(id = us.current_project_id)
        pj.validate_current_dataset_id(id = pj.current_dataset_id)
        ds = ros.Dataset(id = pj.current_dataset_id)
        ds.validate_schema(schema = ds.schema)
        if level not in ds.schema:
            raise ValueError("Level must be in the dataset schema.")

    #################### Start Source objects ####################
    def get_source_object_ids(self, cls: type) -> list:
        """Return a list of all source objects for the Variable."""
        ids = self._get_all_source_object_ids(cls = cls)
        return [cls(id = id) for id in ids]
    
    ## NOTE: THERE ARE NO TARGET OBJECTS FOR VARIABLES