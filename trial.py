"""Representation of a trial object. Subclass of DataObject."""

from data_object import DataObject
from dataclasses import dataclass

@dataclass
class Trial(DataObject):
    id: str
    visit_id: str

    def __init__(self, id: str, visit_id: str) -> None:
        """Initialize the trial object."""
        self.id = id
        self.visit_id = visit_id
        self.get_info()
    
    def get_all_phases(self) -> list[str]:
        """Returns all phases of the trial."""
        table_name = "trial_phase_id"
        parent_name = "trial_id"
        child_name = "phase_id"
        return super().get_all_children(self.id, table_name, parent_name, child_name)
        
    def is_phase(self, phase_id: str) -> bool:
        """Returns whether the phase exists in this trial."""
        table_name = "trial_phase_id"
        parent_name = "trial_id"
        child_name = "phase_id"
        return super().is_child(self.id, phase_id, table_name, parent_name, child_name)

    def get_all_visits(self) -> str:
        """Returns the singular visit_id that this trial is in."""
        return self.visit_id

    def is_visit(self, visit_id: str) -> bool:
        """Returns true if the specified visit_id matches the trial's visit_id."""
        if visit_id == self.visit_id:
            return True
        return False
    
    def get_info(self) -> dict:
        """Returns the information of the trial, including its phases."""
        return {
            "phase_ids": self.get_all_phases()
        }