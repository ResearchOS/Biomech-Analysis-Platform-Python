from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject

class Analysis(PipelineObject):

    prefix = "AN"    

    def __init__(self, current_logsheet_id: str = None): 
        """Initialize the class-specific attributes."""        
        super().__init__()
        self.validate_id_class(current_logsheet_id, "LG")        
        self.current_logsheet_id = current_logsheet_id        
    
    def new_current(name: str):
        from src.ResearchOS.PipelineObjects.logsheet import Logsheet        
        an = Analysis(name = name)
        lg = Logsheet(name = an.name + "_Default")
        an.current_logsheet_id = lg.id                
        return an
        
    def get_logsheets(self) -> list:
        """Return a list of all logsheet objects in the analysis."""
        from src.ResearchOS.PipelineObjects.logsheet import Logsheet
        lg_ids = self._get_all_target_object_ids(cls = Logsheet)
        return [Logsheet(id = lg_id) for lg_id in lg_ids]
    
    def add_logsheet(self, logsheet_id: str):
        """Add a logsheet to the analysis."""
        from src.ResearchOS.PipelineObjects.logsheet import Logsheet
        self.validate_id_class(logsheet_id, Logsheet)
        self._add_target_object_id(logsheet_id, cls = Logsheet)

    def remove_logsheet(self, logsheet_id: str):
        """Remove a logsheet from the analysis."""
        from src.ResearchOS.PipelineObjects.logsheet import Logsheet
        self.validate_id_class(logsheet_id, Logsheet)
        self._remove_target_object_id(logsheet_id, cls = Logsheet)
    
    