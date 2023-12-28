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
    
    #################### Start class-specific attributes ###################

    #################### Start Source objects ####################
    def get_projects(self) -> list:
        """Return a list of project objects that belong to this analysis."""
        from src.ResearchOS.PipelineObjects.project import Project
        pj_ids = self._get_all_source_object_ids(cls = Project)
        return [Project(id = pj_id) for pj_id in pj_ids]

    #################### Start Target objects ####################
    def get_logsheets(self) -> list:
        """Return a list of all logsheet objects in the analysis."""
        from src.ResearchOS.PipelineObjects.logsheet import Logsheet
        lg_ids = self._get_all_target_object_ids(cls = Logsheet)
        return [Logsheet(id = lg_id) for lg_id in lg_ids]
    
    def add_logsheet(self, logsheet_id: str):
        """Add a logsheet to the analysis."""
        from src.ResearchOS.PipelineObjects.logsheet import Logsheet        
        self._add_target_object_id(logsheet_id, cls = Logsheet)

    def remove_logsheet(self, logsheet_id: str):
        """Remove a logsheet from the analysis."""
        from src.ResearchOS.PipelineObjects.logsheet import Logsheet        
        self._remove_target_object_id(logsheet_id, cls = Logsheet)
    
    def get_processes(self) -> list:
        """Return a list of all process objects in the analysis."""
        from src.ResearchOS.PipelineObjects.process import Process
        pr_ids = self._get_all_target_object_ids(cls = Process)
        return [Process(id = pr_id) for pr_id in pr_ids]
    
    def add_process(self, process_id: str):
        """Add a process to the analysis."""
        from src.ResearchOS.PipelineObjects.process import Process        
        self._add_target_object_id(process_id, cls = Process)

    def remove_process(self, process_id: str):
        """Remove a process from the analysis."""
        from src.ResearchOS.PipelineObjects.process import Process        
        self._remove_target_object_id(process_id, cls = Process)