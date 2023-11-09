"""The dataset object."""

from data_object import DataObject

class Dataset(DataObject):
    def __init__(self, id: str) -> None:
        self.id = id

    