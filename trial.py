"""Representation of a trial object. Subclass of DataObject."""

from data_object import DataObject
from dataclasses import dataclass

@dataclass
class Trial(DataObject):
    id: str
    subject_id: str 
    
    def get_all_phases(self) -> list[str]:
        """Returns all phases of the trial."""
        table_name = "trial_phase_id"
        parent_name = "trial_id"
        child_name = "phase_id"
        return super().get_all_children(self.id, table_name, parent_name, child_name)
        
    def is_phase(self, phase_id: str) -> bool:
        """Returns whether the visit is a child of the trial."""
        table_name = "trial_phase_id"
        parent_name = "trial_id"
        child_name = "phase_id"
        return super().is_child(self.id, phase_id, table_name, parent_name, child_name)

    def get_all_visits(self) -> list[str]:
        """Returns all visits of the trial."""
        table_name = "trial_visit_id"
        parent_name = "trial_id"
        child_name = "visit_id"
        return super().get_all_parents(self.id, table_name, parent_name, child_name)

    def is_visit(self, visit_id: str) -> bool:
        """Returns whether the visit is a parent of the trial."""
        table_name = "trial_visit_id"
        parent_name = "trial_id"
        child_name = "visit_id"
        return super().is_parent(self.id, visit_id, table_name, parent_name, child_name)