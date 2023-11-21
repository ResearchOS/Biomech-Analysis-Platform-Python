from objects.data_object import DataObject
from SQL.database_init import DBInitializer
from typing import Union

class Dataset(DataObject):

    _id_prefix: str = "DS"
    _table_name: str = "datasets"

    def __new__(cls, *args, **kwargs):
        return super().__new__(cls, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        # Check if the object already exists.
        if self.uuid in DataObject._instances:
            return
        super().__init__(*args, **kwargs)
        self._subjects = self._get_all_children(self.uuid, "dataset_uuid", "subjects")
        
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

    def remove_subject(self, subject: Union[str, DataObject]) -> None:
        """Remove a subject from the dataset."""
        from subject import Subject
        self._check_type(subject, [str, Subject])
        self._subjects.remove(subject.uuid)
        self.update()

    def add_subject(self, subject: Union[str, DataObject]) -> None:
        """Add a subject to the dataset."""
        from subject import Subject
        self._check_type(subject, [str, Subject])
        self._subjects.append(subject.uuid)
        self.update()

if __name__=="__main__":
    from objects.subject import Subject
    from objects.trial import Trial
    from objects.phase import Phase
    db = DBInitializer()
    
    d1 = Dataset("DS1")
    d1_1 = Dataset("DS1")
    # s1 = Subject(uuid = "SB1", dataset_uuid = "DS1")
    # s2 = Subject(uuid = "SB2", dataset_uuid = "DS1")
    # d1.subjects = ["SB1", "SB2"]

    # BETTER - EITHER OPTION
    s4 = Subject(uuid = "SB4", dataset_uuid = "DS1", dataset = d1)

    t1 = Trial(uuid = "TR1", visit = v1)
    t2 = Trial(uuid = "TR2", visit = v1)
    trials = Subject.find(name = "s1").Trial.find(task = "SLG")

    sql.query.where(name = "x").where(task = "SLG")


    p1 = Phase(uuid = "PH1", trial = [t1, t2])
    p2 = Phase(uuid = "PH2", trial = t1)
    t3.add_phase(p1)

    s4 = d1.add_subject("SB4")
    