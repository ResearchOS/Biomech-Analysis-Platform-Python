from ResearchOS.DataObjects.data_object import DataObject

all_default_attrs = {}

computer_specific_attr_names = []

class Condition(DataObject):

    prefix: str = "CD"

    def __init__(self, **kwargs) -> None:
        if self._initialized:
            return
        super().__init__(**kwargs)