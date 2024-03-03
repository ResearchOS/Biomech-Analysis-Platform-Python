from ResearchOS.DataObjects.data_object import DataObject

all_default_attrs = {}

computer_specific_attr_names = []

class Visit(DataObject):

    prefix: str = "VS"