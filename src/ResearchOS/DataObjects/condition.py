from typing import Any

from ResearchOS.DataObjects.data_object import DataObject

all_default_attrs = {}

computer_specific_attr_names = []

class Condition(DataObject):

    prefix: str = "CN"