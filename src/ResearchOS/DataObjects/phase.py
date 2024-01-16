from ResearchOS import DataObject

from abc import abstractmethod

class Phase(DataObject):
    """Phase class."""

    prefix = "PH"
    logsheet_header: str = None

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Phase)

    #################### Start class-specific attributes ###################

    #################### Start Source objects ####################
    def get_trials(self) -> list:
        """Return a list of trial objects that belong to this phase."""
        from ResearchOS import Trial
        tr_ids = self._get_all_source_object_ids(cls = Trial)
        return [Trial(id = tr_id) for tr_id in tr_ids]
    
    def get_conditions(self) -> list:
        """Return a list of condition objects that belong to this phase."""
        from ResearchOS import Condition
        cn_ids = self._get_all_source_object_ids(cls = Condition)
        return [Condition(id = cn_id) for cn_id in cn_ids]
    
    #################### Start Target objects ####################
    def get_variable_ids(self) -> list:
        """Return a list of variable IDs that belong to this phase."""
        from ResearchOS import Variable
        return self._get_all_target_object_ids(cls = Variable)
    
    def add_variable_id(self, variable_id: str):
        """Add a variable to the phase."""
        from ResearchOS import Variable        
        self._add_target_object_id(variable_id, cls = Variable)

    def remove_variable_id(self, variable_id: str):
        """Remove a variable from the phase."""
        from ResearchOS import Variable        
        self._remove_target_object_id(variable_id, cls = Variable)