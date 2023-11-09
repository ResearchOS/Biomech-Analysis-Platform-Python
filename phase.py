"""Representation of a trial-specific phase object. Subclass of DataObject."""

from data_object import DataObject
from dataclasses import dataclass

@dataclass
class Phase(DataObject):
    trial_phase_id: str
    phase_id: str
    trial_id: str

    def get_all_parents(self) -> list[str]:
        table_name = "trial_phase_id"
        parent_name = "trial_id"
        child_name = "phase_id"
        super().get_all_parents(self.phase_id, table_name, parent_name, child_name)
        
    def is_parent(self, trial_id: str) -> bool:
        """Returns whether the trial is the parent of the phase."""
        table_name = "trial_phase_id"
        parent_name = "trial_id"
        child_name = "phase_id"
        super().is_parent(self.phase_id, trial_id, table_name, parent_name, child_name)