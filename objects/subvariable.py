from objects.data_object import DataObject
from typing import Union

class Subvariable(DataObject):
    """Subvariable class."""

    _uuid_prefix: str = "SV"
    _table_name: str = "subvariables"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)        
        self._variables = self._get_all_parents(self.uuid, "subvariable_uuid", "trial_uuid", "trial_subvariables")        

    @property
    def variables(self) -> DataObject:
        """Return the variable."""
        from variable import Variable
        return Variable(self._variable)
    
    @variables.setter
    def variables(self, value: Union[str, DataObject]) -> None:
        """Set the variable."""
        from variable import Variable
        self._check_type(value, [str, Variable])
        self._variable = value.uuid
        self.update()

    def missing_parent_error(self) -> None:
        """Raise an error if the parent is missing."""
        raise AttributeError("Cannot create a subvariable without a variable.")