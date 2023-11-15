from data_object import DataObject
from typing import Union

class Subject(DataObject):
    """Subject class."""

    _uuid_prefix: str = "SB"
    _table_name: str = "subjects"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # self._visits = self.visits() # Get all visits for the subject.
        self.dataset_uuid = self._get_parent(self.uuid, "subjects", "dataset_uuid") # Get the dataset for the subject.
        self.dataset
    
    @property
    def visits(self):
        """Return all visits."""
        from visit import Visit
        x = [Visit(uuid) for uuid in self._visits]
    
    @visits.setter
    def visits(self, values: list[Union[str, DataObject]] = None) -> None:
        """Set visits. Can provide either a list of visit UUIDs or a list of visit objects."""
        from visit import Visit
        self._check_type(values, [str, Visit])
        self._subjects = self._to_uuids(values)

    @property
    def dataset(self):
        """Return the dataset."""
        from dataset import Dataset
        return Dataset(self.dataset_uuid)
    
    @dataset.setter
    def dataset(self):
        """Set the dataset."""
        raise ValueError("Cannot set the dataset for a subject.")