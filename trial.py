from data_object import DataObject

class Trial(DataObject):
    def __init__(self, *args, **kwargs):
        self._uuid_prefix = "T"
        self._table_name = "trials"
        super().__init__(*args, **kwargs)
        self._phases = self.phases() # Get all phases for the trial.
    
    @property
    def phases(self):
        """Return all phases."""
        from phase import Phase
        # Query the many to many table.
    
    @phases.setter
    def phases(self, values: list[str] = None) -> list:
        """Set phases. Can provide either a list of phase UUIDs or a list of phase objects."""
        if values is not None:
            values = self._input_to_list(values)
            uuids = []
            for value in values:
                if not isinstance(value, str):
                    uuids.append(value.uuid)
                else:
                    uuids.append(value)
                    
            self._phases = uuids
        
        # If no value provided, get all phases from the database.
        cursor = self._conn.cursor()
        cursor.execute(f"SELECT id FROM phases WHERE trial = '{self.uuid}'")
        self._phases = cursor.fetchall()