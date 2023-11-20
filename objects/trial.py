from objects.data_object import DataObject
from typing import Union

class Trial(DataObject):

    _id_prefix: str = "TR"
    _table_name: str = "trials"

    def __new__(cls, uuid, *args, **kwargs):
        return super().__new__(uuid, *args, **kwargs)

    def __init__(self, *args, **kwargs):
        # Check if the object already exists.
        if hasattr(self, "uuid"):
            return
        super().__init__(*args, **kwargs)
        self._phases = self.phases() # Get all phases for the trial.
        self.visit_uuid = self._get_parent(self.uuid, "trials", "visit_uuid")
    
    @property
    def phases(self):
        """Return all phases."""
        from phase import Phase
        return [Phase(uuid) for uuid in self._phases]
    
    @phases.setter
    def phases(self, values: list[str] = None) -> list:
        """Set phases. Can provide either a list of phase UUIDs or a list of phase objects."""
        from phase import Phase
        self._check_type(values, [str, Phase])
        self._phases = self._to_uuids(values)

    @property
    def visit(self):
        """Return the visit."""
        from objects.visit import Visit
        if not self.visit_uuid:
            return None
        return Visit(self.visit_uuid)
    
    @visit.setter
    def visit(self):
        """Cannot set the visit."""
        raise AttributeError("Cannot set the visit from the trial.")
    
    def missing_parent_error(self) -> None:
        """Raise an error if the parent is missing."""
        raise AttributeError("Cannot create a trial without a visit.")