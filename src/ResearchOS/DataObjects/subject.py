from src.ResearchOS.DataObjects import DataObject

from abc import abstractmethod

class Subject(DataObject):
    """Subject class."""

    prefix: str = "SJ"
    logsheet_header: str = None

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Subject)

    #################### Start class-specific attributes ###################

    #################### Start Source objects ####################
    def get_datasets(self) -> list:
        """Return a list of dataset objects that belong to this subject."""
        from src.ResearchOS.DataObjects.dataset import Dataset
        ds_ids = self._get_all_source_object_ids(cls = Dataset)
        return [Dataset(id = ds_id) for ds_id in ds_ids]
    
    #################### Start Target objects ####################
    def get_visits(self) -> list:
        """Return a list of visit objects that belong to this subject."""
        from src.ResearchOS.DataObjects.visit import Visit
        vs_ids = self._get_all_target_object_ids(cls = Visit)
        return [Visit(id = vs_id) for vs_id in vs_ids]
    
    def add_visit_id(self, visit_id: str):
        """Add a visit to the subject."""
        from src.ResearchOS.DataObjects.visit import Visit        
        self._add_target_object_id(visit_id, cls = Visit)

    def remove_visit_id(self, visit_id: str):
        """Remove a visit from the subject."""
        from src.ResearchOS.DataObjects.visit import Visit        
        self._remove_target_object_id(visit_id, cls = Visit)