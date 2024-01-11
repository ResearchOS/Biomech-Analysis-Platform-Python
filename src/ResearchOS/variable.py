from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.PipelineObjects.pipeline_object import PipelineObject

from abc import abstractmethod

default_instance_attrs = {}
default_instance_attrs["hard_coded_value"] = None

class Variable(DataObject, PipelineObject):
    """Variable class."""

    prefix: str = "VR"           

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Variable)
    
    #################### Start class-specific attributes ###################

    #################### Start Source objects ####################
    def get_source_object_ids(self, cls: type) -> list:
        """Return a list of all source objects for the Variable."""
        from ResearchOS import Variable
        ids = self._get_all_source_object_ids(cls = cls)
        return [cls(id = id) for id in ids]
    
    ## NOTE: THERE ARE NO TARGET OBJECTS FOR VARIABLES