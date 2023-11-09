"""Representation of a trial-specific phase object. Subclass of DataObject."""

from data_object import DataObject
from dataclasses import dataclass

@dataclass
class Phase(DataObject):
    """The phase object. Independent of the trial object."""
    trial_phase_id: str
    phase_id: str
    trial_id: str

    def get_all_trials(self) -> list[str]:
        """Get all trials that have this phase."""
        table_name = "trial_phase_id"
        parent_name = "trial_id"
        child_name = "phase_id"
        super().get_all_parents(self.phase_id, table_name, parent_name, child_name)
        
    def is_trial(self, trial_id: str) -> bool:
        """Returns whether the trial contains this phase."""
        if trial_id == self.trial_id:
            return True
        return False

    def get_all_vars(self) -> list[str]:
        """Returns all variables of the phase."""
        table_name = "phase_variable_id"
        parent_name = "phase_id"
        child_name = "variable_id"
        super().get_all_children(self.phase_id, table_name, parent_name, child_name)

    def is_var(self, var_id: str) -> bool:
        """Returns whether the phase contains the variable."""
        table_name = "phase_variable_id"
        parent_name = "phase_id"
        child_name = "variable_id"
        super().is_child(self.phase_id, var_id, table_name, parent_name, child_name)

    def get_info(self) -> dict:
        """Returns the information of the phase."""
        return {
            "variable_ids": self.get_all_vars()
        }