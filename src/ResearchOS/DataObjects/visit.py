from ResearchOS.DataObjects import DataObject
from typing import Union

from datetime import datetime

import numpy as np

class Visit(DataObject):

    _id_prefix: str = "VT"
    _table_name: str = "visits"
    logsheet_header_type: int = 0

    def check_valid_attrs(self):
        if type(self.date) is not datetime:
            raise ValueError


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._trials = self.trials() # Get all trials for the visit.
    
    @property
    def trials(self):
        """Return all trials."""
        from objects.trial import Trial
        x = [Trial(uuid) for uuid in self._trials]
    
    @trials.setter
    def trials(self, values: list[Union[str, DataObject]] = None) -> list:
        """Set trials. Can provide either a list of trial UUIDs or a list of trial objects."""
        from objects.trial import Trial
        self._check_type(values, [str, Trial])
        self._trials = self._to_uuids(values)

    @property
    def subject(self):
        """Return the subject."""
        from objects.subject import Subject
        if not self.subject_uuid:
            return None
        return Subject(self.subject_uuid)
    
    @subject.setter
    def subject(self):
        """Cannot set the subject."""
        raise AttributeError("Cannot set the subject from the visit.")
    
    def missing_parent_error(self) -> None:
        """Raise an error if the parent is missing."""
        raise AttributeError("Cannot create a visit without a subject.")