from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject

from abc import abstractmethod

class Logsheet(PipelineObject):

    prefix = "LG"

    @abstractmethod
    def get_all_ids() -> list[str]:
        return super().get_all_ids(Logsheet)

    #################### Start class-specific attributes ###################
    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        attrs = {}
        attrs["logsheet_path"] = "" # Path to logsheet.
        attrs["logsheet_headers"] = [] # List of tuples. Each tuple is: (header_name, type, variable ID)
        attrs["num_header_rows"] = -1 # Integer
        attrs["class_column_names"] = {} # dict where the keys are logsheet column names (first column of logsheet_headers), values are class types        
        super().__init__(attrs = attrs)

    def json_translate_XXX(self):
        """Convert the attribute from JSON to the proper data type/format, if json.loads is not sufficient.
        XXX is the exact name of the attribute. Method name must follow this format."""        

    def validate_num_header_rows(self, num_header_rows):
        """Validate the number of header rows. If it is not valid, the value is rejected."""
        if not isinstance(num_header_rows, int):
            raise ValueError("Num header rows must be an integer!")
        if num_header_rows<0:
            raise ValueError("Num header rows must be positive!")

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
    # lg.test = 1
    lg.num_header_rows = -1
    print(lg.num_header_rows)