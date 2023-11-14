from data_object import DataObject

class Dataset(DataObject):
    def __init__(self, id: str =None, name=None, description=None, uuid=None, created_at=None, updated_at=None):        
        self.name = name
        self.description = description
        self.uuid = uuid
        super().__init__(id, created_at, updated_at) # Created/updated time.
        if created_at or updated_at is not None:
            time = self.current_timestamp()
            self.created_at = time
            self.updated_at = time

    def subjects(self):
        """Return all subjects."""
        self.get_all_children(self.id, 'subjects', 'subject_id')