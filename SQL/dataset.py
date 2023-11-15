from data_object import DataObject
from database_init import DBInitializer
from typing import Union

from json import load 

class Dataset(DataObject):

    _uuid_prefix: str = "DS"
    _table_name: str = "datasets"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._subjects = self._get_all_parents(self.uuid, "subjects", "dataset_uuid")

    @property
    def subjects(self) -> list[DataObject]:
        """Return all subjects."""
        from subject import Subject
        return [Subject(uuid) for uuid in self._subjects]
    
    @subjects.setter
    def subjects(self, values: list[Union[str, DataObject]] = None) -> None:
        """Set subjects. Can provide either a list of subject UUIDs or a list of subject objects."""
        from subject import Subject
        self._check_type(values, [str, Subject])
        self._subjects = self._to_uuids(values)  

if __name__=="__main__":
    db = DBInitializer()
    d1 = Dataset("DS1")
    d1.subjects = ["SB1", "SB2"]