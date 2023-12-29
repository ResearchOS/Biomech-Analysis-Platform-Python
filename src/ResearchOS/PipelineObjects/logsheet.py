from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject

from abc import abstractmethod

class Logsheet(PipelineObject):

    prefix = "LG"

    def __init__(self, **kwargs):
        attrs = {}
        attrs["logsheet_path"] = "" # Path to logsheet.
        attrs["logsheet_headers"] = [] # List of tuples. Each tuple is: (header_name, type, variable ID)
        attrs["num_header_rows"] = -1 # Integer
        attrs["class_column_names"] = {} # dict where the keys are logsheet column names (first column of logsheet_headers), values are class types        
        super().__init__(attrs = attrs)

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Logsheet)

    #################### Start class-specific attributes ###################
    # self.logsheet_path = None
    # self.logsheet_headers = None # Includes header names, types, and variable IDs.
    # self.num_header_rows = None
    # self.class_column_names = None # dict or tuple mapping logsheet column names to class names to save with the current data schema.

    #################### Start Source objects ####################
    def get_analyses(self) -> list:
        """Return a list of analysis objects that belong to this logsheet."""
        from src.ResearchOS.PipelineObjects.analysis import Analysis
        an_ids = self._get_all_source_object_ids(cls = Analysis)
        return [Analysis(id = an_id) for an_id in an_ids]
    
    #################### Start Target objects ####################
    def get_variable_ids(self) -> list:
        """Return a list of variable IDs that belong to this logsheet."""
        from src.ResearchOS.variable import Variable
        return self._get_all_target_object_ids(cls = Variable)
    
    def add_variable_id(self, variable_id: str):
        """Add a variable to the logsheet."""
        # TODO: Mapping between variable ID and column header.
        from src.ResearchOS.variable import Variable
        self._add_target_object_id(variable_id, cls = Variable)

    def remove_variable_id(self, variable_id: str):
        """Remove a variable from the logsheet."""
        from src.ResearchOS.variable import Variable
        self._remove_target_object_id(variable_id, cls = Variable)

if __name__=="__main__":
    lg = Logsheet(id = "LG000000_001")
    print(lg.num_header_rows)
    lg.num_header_rows = 3
    print(lg.num_header_rows)