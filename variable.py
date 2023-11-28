from research_object import ResearchObject
from typing import Union

class Variable(ResearchObject):
    """Variable class."""

    _id_prefix: str = "VR"
    _table_name: str = "variables"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._phases = self._get_all_parents(self.uuid, "variable_uuid", "phase_uuid", "phase_variables")

    @property
    def phases(self) -> list[ResearchObject]:
        """Return all phases."""
        from phase import Phase
        return [Phase(uuid) for uuid in self._phases]
    
    @phases.setter
    def phases(self, values: list[Union[str, ResearchObject]] = None) -> None:
        """Set phases. Can provide either a list of phase UUIDs or a list of phase objects."""
        from phase import Phase
        self._check_type(values, [str, Phase])
        self._phases = self._to_uuids(values)

    def missing_parent_error(self) -> None:
        """Raise an error if the parent is missing."""
        pass