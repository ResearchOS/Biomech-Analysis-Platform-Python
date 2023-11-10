"""Representation of a trial-specific phase object. Subclass of DataObject."""

from data_object import DataObject
from dataclasses import dataclass

from trial import Trial
from variable import Variable

@dataclass
class Phase(DataObject):
    """The phase object. Independent of the trial object."""
    id: str

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

    def get_all_vars(self, trial_id: list[str]) -> list[Variable]:
        """Returns all variables of the phase. If trial_id not specified, returns it across all trials."""
        trial_id = self.input_to_list(trial_id)
        table_name = "phase_data"
        parent_name = "phase_id"
        child_name = "variable_id"
        all_trials = super().get_all_children(self.id, table_name, parent_name, child_name)
        # Convert the output to a list of Trials
        vars_in_trials = []
        for trial in all_trials:
            curr_trial = Trial(trial)
            if curr_trial.id in trial_id:
                vars_in_trials.append(trial)
        return vars_in_trials

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