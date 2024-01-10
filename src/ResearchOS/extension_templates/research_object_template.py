from src.ResearchOS.DataObjects.data_object import DataObject
from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from src.ResearchOS.action import Action

from abc import abstractmethod
from typing import Any
import json

default_instance_attrs = {}
default_instance_attrs["attr1"] = type

default_abstract_attrs = {}
default_abstract_attrs["attr1"] = type

class Template(PipelineObject, DataObject):

    prefix = "TP"

    def get_default_attrs(self):
        """Return a dictionary of default instance or abstract attributes, as appropriate for this object."""
        if self.is_instance_object():
            return default_instance_attrs
        return default_abstract_attrs
    
    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Template)
    
    def __str__(self):
        return super().__str__(self.get_default_attrs().keys(), self.__dict__)
    
    def __init__(self, **kwargs) -> None:
        super().__init__(attrs = self.get_default_attrs(), **kwargs)

    #################### Start class-specific attributes ###################
    def validate_attr1(self, attr1: type) -> None:
        """Validate the value of attr1. If one of the checks for validity fail, raise a ValueError (or any error besides AttributeError).
        "attr1" must match the attribute name exactly.
        This method is only called during __setattr__, while the attribute is being set."""        
        raise NotImplementedError

    def from_json_attr1(self) -> str:
        """Translate the value of attr1 from a JSON-friendly format.
        "attr1" must match the attribute name exactly.
        This method is called when loading an attribute from the research_objects_attributes table."""
        raise NotImplementedError
        return json.loads(self.attr1, indent = 4)

    def to_json_attr1(self) -> Any:
        """Translate the value of attr1 into a JSON-friendly format.
        "attr1" must match the attribute name exactly.
        This method is called when storing an attribute in the research_objects_attributes table."""
        raise NotImplementedError
        return json.dumps(self.attr1, indent = 4)

    def store_attr1(self, attr1: type, action: Action) -> Action:
        """Custom method to store the value of attr1. "attr1" must match the attribute name exactly.
        This method is called when storing an attribute in the research_objects_attributes table.
        action.add_sql_query(sqlquery) to add to the list of sql_queries to be executed."""
        sqlquery = ""
        action.add_sql_query(sqlquery)
        return action
        
    #################### Start Source objects ####################
    def get_template_source_objects(self) -> list:
        """Return a list of template's source objects associated with this instance of the Template object."""
        from src.ResearchOS.extension_templates.template_source_object import TemplateSourceObject
        tso_ids = self._get_all_source_object_ids(cls = TemplateSourceObject)
        return self._gen_obj_or_none(tso_ids, TemplateSourceObject)
    
    #################### Start Target objects ####################
    def get_template_target_objects(self) -> list:
        """Return a list of template's target objects associated with this instance of the Template object."""
        from src.ResearchOS.extension_templates.template_target_object import TemplateTargetObject        
        tto_ids = self._get_all_target_object_ids(cls = TemplateTargetObject)
        return self._gen_obj_or_none(tto_ids, TemplateTargetObject)
    
    def add_template_target_object(self, template_target_object_id: str):
        """Add a template target object to the template."""
        # Import the target object class here.
        from src.ResearchOS.extension_templates.template_target_object import TemplateTargetObject
        self._add_target_object_id(template_target_object_id, cls = TemplateTargetObject)

    def remove_template_target_object(self, template_target_object_id: str):
        """Remove a template target object from the template."""
        # Import the target object class here.
        from src.ResearchOS.extension_templates.template_target_object import TemplateTargetObject
        self._remove_target_object_id(template_target_object_id, cls = TemplateTargetObject)

    #################### Start class-specific methods ###################
    def template_method(self):
        """Template method."""
        pass