from data_object import DataObject
from trial import Trial

class Visit(DataObject):
    def __init__(self, *args, **kwargs):
        self._uuid_prefix = "V"
        self._table_name = "visits"
        super().__init__(*args, **kwargs)
        self._trials = self.trials() # Get all trials for the visit.
    
    @property
    def trials(self):
        """Return all trials."""
        x = [Trial(uuid) for uuid in self._trials]
    
    @trials.setter
    def trials(self, values: list[str] = None) -> list:
        """Set trials. Can provide either a list of trial UUIDs or a list of trial objects."""
        if values is not None:
            values = self._input_to_list(values)
            uuids = []
            for value in values:
                if not isinstance(value, str):
                    uuids.append(value.uuid)
                else:
                    uuids.append(value)
                    
            self._trials = uuids
        
        # If no value provided, get all trials from the database.
        cursor = self._conn.cursor()
        cursor.execute(f"SELECT id FROM trials WHERE visit = '{self.uuid}'")
        self._trials = cursor.fetchall()