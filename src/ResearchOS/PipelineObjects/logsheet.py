from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject

class Logsheet(PipelineObject):

    prefix = "LG"

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