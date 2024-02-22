from typing import Any

from ResearchOS.DataObjects.data_object import DataObject

all_default_attrs = {}

complex_attrs_list = []

class Condition(DataObject):

    prefix: str = "CN"