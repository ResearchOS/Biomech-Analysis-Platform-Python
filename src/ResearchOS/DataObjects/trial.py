from src.ResearchOS.DataObjects import DataObject
from typing import Union

class Trial(DataObject):

    preifx = "TR"
    logsheet_header: str = None

    #################### Start class-specific attributes ###################

    #################### Start Source objects ####################
    def get_visits(self) -> list:
        """Return a list of visit objects that belong to this trial."""
        from src.ResearchOS.DataObjects.visit import Visit
        vs_ids = self._get_all_source_object_ids(cls = Visit)
        return [Visit(id = vs_id) for vs_id in vs_ids]
    
    #################### Start Target objects ####################
    def get_phase_ids(self) -> list:
        """Return a list of phase object IDs that belong to this trial."""
        from src.ResearchOS.DataObjects.phase import Phase
        ph_ids = self._get_all_target_object_ids(cls = Phase)
        return [Phase(id = ph_id) for ph_id in ph_ids]
    
    def add_phase_id(self, phase_id: str):
        """Add a phase to the trial."""
        from src.ResearchOS.DataObjects.phase import Phase        
        self._add_target_object_id(phase_id, cls = Phase)

    def remove_phase_id(self, phase_id: str):
        """Remove a phase from the trial."""
        from src.ResearchOS.DataObjects.phase import Phase        
        self._remove_target_object_id(phase_id, cls = Phase)