from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject

from abc import abstractmethod

default_instance_attrs = {}
default_instance_attrs["conditions"] = {}

class Subset(PipelineObject):
    """Provides rules to select a subset of data from a dataset."""
    
    prefix = "SS"

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Subset)

    #################### Start class-specific attributes ###################
    def add_criteria(self, var_id: str, value, logic: str) -> None:
        """Add a criterion to the subset.
        Possible values for logic are: ">", "<", ">=", "<=", "==", "!=", "in", "not in", "is", "is not", "contains", "not contains"."""
        from src.ResearchOS.variable import Variable
        logic_options = [">", "<", ">=", "<=", "==", "!=", "in", "not in", "is", "is not", "contains", "not contains"]
        if logic not in logic_options:
            raise ValueError("Invalid logic value.")
        if var_id not in Variable.get_all_ids():
            raise ValueError("Invalid variable ID.")
        self.criteria.append((var_id, value, logic))

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