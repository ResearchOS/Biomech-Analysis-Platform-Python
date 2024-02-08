from ResearchOS.PipelineObjects.pipeline_object import PipelineObject

from abc import abstractmethod

default_attrs = {}
default_attrs["current_logsheet_id"] = None

class Analysis(PipelineObject):

    prefix = "AN"        

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Analysis)   
    
    def new_current(name: str):
        from ResearchOS import Logsheet        
        an = Analysis(name = name)
        lg = Logsheet(name = an.name + "_Default")
        an.current_logsheet_id = lg.id                
        return an
    
    def __str__(self):        
        return super().__str__(self.get_default_attrs().keys(), self.__dict__)
    
    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        super().__init__(default_attrs, **kwargs)
    
    #################### Start class-specific attributes ###################

    #################### Start Source objects ####################
    def get_projects(self) -> list:
        """Return a list of project objects that belong to this analysis."""
        from ResearchOS import Project
        pj_ids = self._get_all_source_object_ids(cls = Project)
        return [Project(id = pj_id) for pj_id in pj_ids]

    #################### Start Target objects ####################
    def get_logsheets(self) -> list:
        """Return a list of all logsheet objects in the analysis."""
        from ResearchOS import Logsheet
        lg_ids = self._get_all_target_object_ids(cls = Logsheet)
        return [Logsheet(id = lg_id) for lg_id in lg_ids]
    
    def add_logsheet(self, logsheet_id: str):
        """Add a logsheet to the analysis."""
        from ResearchOS import Logsheet        
        self._add_target_object_id(logsheet_id, cls = Logsheet)

    def remove_logsheet(self, logsheet_id: str):
        """Remove a logsheet from the analysis."""
        from ResearchOS import Logsheet        
        self._remove_target_object_id(logsheet_id, cls = Logsheet)
    
    def get_processes(self) -> list:
        """Return a list of all process objects in the analysis."""
        from ResearchOS import Process
        pr_ids = self._get_all_target_object_ids(cls = Process)
        return [Process(id = pr_id) for pr_id in pr_ids]
    
    def add_process(self, process_id: str):
        """Add a process to the analysis."""
        from ResearchOS import Process        
        self._add_target_object_id(process_id, cls = Process)

    def remove_process(self, process_id: str):
        """Remove a process from the analysis."""
        from ResearchOS import Process        
        self._remove_target_object_id(process_id, cls = Process)