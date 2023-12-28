from src.ResearchOS.DataObjects import DataObject
from typing import Union

from datetime import datetime

from abc import abstractmethod

class Visit(DataObject):

    prefix: str = "VS"
    logsheet_header: str = None

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Visit)

    #################### Start class-specific attributes ###################

    #################### Start Source objects ####################
    def get_subjects(self) -> list:
        """Return a list of subject objects that belong to this visit."""
        from src.ResearchOS.DataObjects.subject import Subject
        sj_ids = self._get_all_source_object_ids(cls = Subject)
        return [Subject(id = sj_id) for sj_id in sj_ids]
    
    #################### Start Target objects ####################
    def get_trials(self) -> list:
        """Return a list of trial objects that belong to this visit."""
        from src.ResearchOS.DataObjects.trial import Trial
        tr_ids = self._get_all_target_object_ids(cls = Trial)
        return [Trial(id = tr_id) for tr_id in tr_ids]
    
    def add_trial_id(self, trial_id: str):
        """Add a trial to the visit."""
        from src.ResearchOS.DataObjects.trial import Trial        
        self._add_target_object_id(trial_id, cls = Trial)

    def remove_trial_id(self, trial_id: str):
        """Remove a trial from the visit."""
        from src.ResearchOS.DataObjects.trial import Trial        
        self._remove_target_object_id(trial_id, cls = Trial)