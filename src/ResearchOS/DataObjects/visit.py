from ResearchOS import DataObject

from abc import abstractmethod

default_attrs = {}

class Visit(DataObject):

    prefix: str = "VS"
    logsheet_header: str = None

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Visit)
    
    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        super().__init__(default_attrs, **kwargs)

    #################### Start class-specific attributes ###################

    #################### Start Source objects ####################
    def get_subjects(self) -> list:
        """Return a list of subject objects that belong to this visit."""
        from ResearchOS import Subject
        sj_ids = self._get_all_source_object_ids(cls = Subject)
        return [Subject(id = sj_id) for sj_id in sj_ids]
    
    #################### Start Target objects ####################
    def get_trials(self) -> list:
        """Return a list of trial objects that belong to this visit."""
        from ResearchOS import Trial
        tr_ids = self._get_all_target_object_ids(cls = Trial)
        return [Trial(id = tr_id) for tr_id in tr_ids]
    
    def add_trial_id(self, trial_id: str):
        """Add a trial to the visit."""
        from ResearchOS import Trial        
        self._add_target_object_id(trial_id, cls = Trial)

    def remove_trial_id(self, trial_id: str):
        """Remove a trial from the visit."""
        from ResearchOS import Trial        
        self._remove_target_object_id(trial_id, cls = Trial)

    def get_conditions(self) -> list:
        """Return a list of condition objects that belong to this visit."""
        from ResearchOS import Condition
        cn_ids = self._get_all_target_object_ids(cls = Condition)
        return [Condition(id = cn_id) for cn_id in cn_ids]
    
    def add_condition_id(self, condition_id: str):
        """Add a condition to the visit."""
        from ResearchOS import Condition        
        self._add_target_object_id(condition_id, cls = Condition)

    def remove_condition_id(self, condition_id: str):
        """Remove a condition from the visit."""
        from ResearchOS import Condition        
        self._remove_target_object_id(condition_id, cls = Condition)