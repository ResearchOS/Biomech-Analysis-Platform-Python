from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject

class Subset(PipelineObject):
    """Provides rules to select a subset of data from a dataset."""
    
    prefix = "SS"

    #################### Start class-specific attributes ###################

    #################### Start Source objects ####################
    def get_processes(self) -> list:
        """Return a list of process objects that belong to this subset."""
        from src.ResearchOS.PipelineObjects.process import Process
        pr_ids = self._get_all_source_object_ids(cls = Process)
        return [Process(id = pr_id) for pr_id in pr_ids]
    
    def get_plots(self) -> list:
        """Return a list of plot objects that belong to this subset."""
        from src.ResearchOS.PipelineObjects.plot import Plot
        pl_ids = self._get_all_source_object_ids(cls = Plot)
        return [Plot(id = pl_id) for pl_id in pl_ids]
    
    def get_logsheets(self) -> list:
        """Return a list of logsheet objects that belong to this subset."""
        from src.ResearchOS.PipelineObjects.logsheet import Logsheet
        lg_ids = self._get_all_source_object_ids(cls = Logsheet)
        return [Logsheet(id = lg_id) for lg_id in lg_ids]
    
    #################### Start Target objects ####################
    # TODO: Does anything go here? Do subsets point to anything?