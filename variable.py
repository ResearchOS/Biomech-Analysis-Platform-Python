from data_object import DataObject
from typing import Union

class Variable(DataObject):
    """Variable class."""

    _uuid_prefix: str = "VR"
    _table_name: str = "variables"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._subvariables = self._get_all_children(self.uuid, "variable_uuid", "subvariables")
        self._phases = self._get_all_parents(self.uuid, "variable_uuid", "phase_uuid", "phase_variables")

    @property
    def subvariables(self) -> list[DataObject]:
        """Return all subvariables."""
        from subvariable import Subvariable
        return [Subvariable(uuid) for uuid in self._subvariables]
    
    @subvariables.setter
    def subvariables(self, values: list[Union[str, DataObject]] = None) -> None:
        """Set subvariables. Can provide either a list of subvariable UUIDs or a list of subvariable objects."""
        from subvariable import Subvariable
        self._check_type(values, [str, Subvariable])
        self._subvariables = self._to_uuids(values)

    @property
    def phases(self) -> list[DataObject]:
        """Return all phases."""
        from phase import Phase
        return [Phase(uuid) for uuid in self._phases]
    
    @phases.setter
    def phases(self, values: list[Union[str, DataObject]] = None) -> None:
        """Set phases. Can provide either a list of phase UUIDs or a list of phase objects."""
        from phase import Phase
        self._check_type(values, [str, Phase])
        self._phases = self._to_uuids(values)

    def remove_subvariable(self, subvariable: Union[str, DataObject]) -> None:
        """Remove a subvariable from the variable."""
        from subvariable import Subvariable
        self._check_type(subvariable, [str, Subvariable])
        self._subvariables.remove(subvariable.uuid)
        self.update()

    def add_subvariable(self, subvariable: Union[str, DataObject]) -> None:
        """Add a subvariable to the variable."""
        from subvariable import Subvariable
        self._check_type(subvariable, [str, Subvariable])
        self._subvariables.append(subvariable.uuid)
        self.update()