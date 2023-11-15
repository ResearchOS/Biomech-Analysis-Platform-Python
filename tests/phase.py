from data_object import DataObject
from typing import Union

class Phase(DataObject):
    """Phase class."""

    _uuid_prefix: str = "PH"
    _table_name: str = "phases"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._variables = self._get_all_children(self.uuid, "phase_uuid", "phase_variables")
        self._trials = self._get_all_parents(self.uuid, "phase_uuid", "trial_uuid", "trial_phases")

    @property
    def variables(self) -> list[DataObject]:
        """Return all variables."""
        from variable import Variable
        return [Variable(uuid) for uuid in self._variables]
    
    @variables.setter
    def variables(self, values: list[Union[str, DataObject]] = None) -> None:
        """Set variables. Can provide either a list of variable UUIDs or a list of variable objects."""
        from variable import Variable
        self._check_type(values, [str, Variable])
        self._variables = self._to_uuids(values)

    @property
    def trials(self) -> list[DataObject]:
        """Return all trials."""
        from trial import Trial
        return [Trial(uuid) for uuid in self._trials]
    
    @trials.setter
    def trials(self, values: list[Union[str, DataObject]] = None) -> None:
        """Set trials. Can provide either a list of trial UUIDs or a list of trial objects."""
        from trial import Trial
        self._check_type(values, [str, Trial])
        self._trials = self._to_uuids(values)

    def remove_variable(self, variable: Union[str, DataObject]) -> None:
        """Remove a variable from the phase."""
        from variable import Variable
        self._check_type(variable, [str, Variable])
        self._variables.remove(variable.uuid)
        self.update()