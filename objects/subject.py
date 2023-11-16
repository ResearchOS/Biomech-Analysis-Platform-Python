from objects.data_object import DataObject
from typing import Union

class Subject(DataObject):
    """Subject class."""

    _uuid_prefix: str = "SB"
    _table_name: str = "subjects"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._visits_list = self._get_all_children(self.uuid, "subject_uuid", "visits") # Get all visits for the subject.
        self.dataset_uuid = self._get_parent(self.uuid, "subjects", "dataset_uuid") # Get the dataset for the subject.
    
    @property
    def visits(self):
        """Return all visits."""
        from visit import Visit
        x = [Visit(uuid) for uuid in self._visits_list]
    
    @visits.setter
    def visits(self, values: list[Union[str, DataObject]] = None) -> None:
        """Set visits. Can provide either a list of visit UUIDs or a list of visit objects."""
        from visit import Visit
        self._check_type(values, [str, Visit])
        self._visits_list = self._to_uuids(values)

    @property
    def dataset(self):
        """Return the dataset."""
        from objects.dataset import Dataset
        if not self.dataset_uuid:
            return None
        return Dataset(self.dataset_uuid)
    
    @dataset.setter
    def dataset(self):
        """Set the dataset."""
        raise AttributeError("Cannot set the dataset from a subject.")
    
    def add_visit(self, visit: Union[str, DataObject]) -> None:
        """Add a visit to the subject."""
        from visit import Visit
        self._check_type(visit, [str, Visit])
        self._visits_list.append(visit.uuid)
        self.update()

    def remove_visit(self, visit: Union[str, DataObject]) -> None:
        """Remove a visit from the subject."""
        from visit import Visit
        self._check_type(visit, [str, Visit])
        self._visits_list.remove(visit.uuid)
        self.update()    

    def missing_parent_error(self) -> None:
        """Raise an error if the parent is missing."""
        pass