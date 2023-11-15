from data_object import DataObject
from visit import Visit

class Subject(DataObject):
    def __init__(self, *args, **kwargs):
        self._uuid_prefix = "SB"
        self._table_name = "subjects"
        super().__init__(*args, **kwargs)
        self._visits = self.visits() # Get all visits for the subject.
        self._dataset = self.dataset() # Get the dataset for the subject.
    
    @property
    def visits(self):
        """Return all visits."""
        x = [Visit(uuid) for uuid in self._visits]
    
    @visits.setter
    def visits(self, values: list[str] = None) -> list:
        """Set visits. Can provide either a list of visit UUIDs or a list of visit objects."""
        if values is not None:
            values = self._input_to_list(values)
            uuids = []
            for value in values:
                if not isinstance(value, str):
                    uuids.append(value.uuid)
                else:
                    uuids.append(value)
                    
            self._visits = uuids
        
        # If no value provided, get all visits from the database.
        cursor = self._conn.cursor()
        cursor.execute(f"SELECT id FROM visits WHERE subject = '{self.uuid}'")
        self._visits = cursor.fetchall()