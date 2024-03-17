from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ResearchOS.research_object import ResearchObject

from ResearchOS.action import Action
from ResearchOS.default_attrs import DefaultAttrs

class Validator():
    def __init__(self, research_object: "ResearchObject", action = Action):
        self.robj = research_object
        self.default_attrs = DefaultAttrs(research_object).default_attrs
        self.action = action

    def validate(self, attrs: dict, default_attrs: dict):
        for key, value in attrs.items():
            validate_method = getattr(self.robj, f'validate_{key}', None)
            if validate_method:
                validate_method(value, self.action, default_attrs[key])