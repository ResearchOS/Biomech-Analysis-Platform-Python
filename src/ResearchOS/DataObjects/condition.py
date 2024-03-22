from ResearchOS.DataObjects.data_object import DataObject

class Condition(DataObject):

    prefix: str = "CD"

    def __init__(self, **kwargs) -> None:
        if self._initialized:
            return
        super().__init__(**kwargs)