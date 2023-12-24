from ResearchOS.DataObjects import DataObject
from typing import Union

class Phase(DataObject):
    """Phase class."""

    _id_prefix: str = "PH"
    _table_name: str = "phases"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._variables_list = self._get_all_children(self.uuid, "phase_uuid", "phases")
        self._trials_list = self._get_all_parents(self.uuid, "phase_uuid", "trial_uuid", "phases")

    @property
    def variables(self) -> list[DataObject]:
        """Return all variables."""
        from variable import Variable
        return [Variable(uuid) for uuid in self._variables_list]
    
    @variables.setter
    def variables(self, values: list[Union[str, DataObject]] = None) -> None:
        """Set variables. Can provide either a list of variable UUIDs or a list of variable objects."""
        from variable import Variable
        self._check_type(values, [str, Variable])
        self._variables_list = self._to_uuids(values)

    @property
    def trials(self) -> list[DataObject]:
        """Return all trials."""
        from trial import Trial
        return [Trial(uuid) for uuid in self._trials_list]
    
    @trials.setter
    def trials(self, values: list[Union[str, DataObject]] = None) -> None:
        """Set trials. Can provide either a list of trial UUIDs or a list of trial objects."""
        from trial import Trial
        self._check_type(values, [str, Trial])
        self._trials_list = self._to_uuids(values)

    def remove_variable(self, variable: Union[str, DataObject]) -> None:
        """Remove a variable from the phase."""
        from variable import Variable
        self._check_type(variable, [str, Variable])
        self._variables.remove(variable.uuid)
        self.update()

    def missing_parent_error(self) -> None:
        """Raise an error if the parent is missing."""
        pass