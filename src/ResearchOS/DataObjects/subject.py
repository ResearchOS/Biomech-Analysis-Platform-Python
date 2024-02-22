from ResearchOS.DataObjects.data_object import DataObject

all_default_attrs = {}

complex_attrs_list = []

class Subject(DataObject):
    """Subject class."""

    prefix: str = "SJ"