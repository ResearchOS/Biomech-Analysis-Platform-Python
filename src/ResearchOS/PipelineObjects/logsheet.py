from src.ResearchOS.PipelineObjects.pipeline_object import PipelineObject

from abc import abstractmethod

# Defaults should be of the same type as the expected values.
default_attrs = {}
default_attrs["logsheet_path"] = ""
default_attrs["logsheet_headers"] = []
default_attrs["num_header_rows"] = -1
default_attrs["class_column_names"] = {}

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
        if self.is_instance_object():
            attrs = default_attrs
        super().__init__(attrs = attrs, **kwargs)

    def __str__(self):
        return super().__str__(default_attrs.keys(), self.__dict__)

    def json_translate_logsheet_headers(self) -> list:
        """Convert the attribute from JSON to the proper data type/format, if json.loads is not sufficient.
        XXX is the exact name of the attribute. Method name must follow this format."""        
        pass

    def json_translate_class_column_names(self) -> dict:
        """Convert the attribute from JSON to the proper data type/format, if json.loads is not sufficient.
        XXX is the exact name of the attribute. Method name must follow this format."""        
        pass

    def validate_num_header_rows(self, num_header_rows: int):
        """Validate the number of header rows. If it is not valid, the value is rejected."""        
        if not isinstance(num_header_rows, int):
            raise ValueError("Num header rows must be an integer!")
        if num_header_rows<0:
            raise ValueError("Num header rows must be positive!")
        if num_header_rows % 1 != 0:
            raise ValueError("Num header rows must be an integer!")
        
    def validate_logsheet_path(self, path: str):
        """Validate the logsheet path."""
        # 1. Check that the path exists in the file system.
        import os
        if not os.path.exists(path):
            raise ValueError("Specified path does not exist!")
        # 2. Check that the path is a file.
        if not os.path.isfile(path):
            raise ValueError("Specified path is not a file!")
        # 3. Check that the file is a CSV.
        if not path.endswith(".csv"):
            raise ValueError("Specified file is not a CSV!")
        # 4. Check that the file is not empty.
        if os.stat(path).st_size == 0:
            raise ValueError("Specified file is empty!")
        
    def validate_logsheet_headers(self, headers: list):
        """Validate the logsheet headers."""
        # 1. Check that the headers are a list.
        if not isinstance(headers, list):
            raise ValueError("Headers must be a list!")
        # 2. Check that the headers are a list of tuples.
        for header in headers:
            if not isinstance(header, tuple):
                raise ValueError("Headers must be a list of tuples!")
            # 3. Check that each header tuple has 3 elements.        
            if len(header) != 3:
                raise ValueError("Each header tuple must have 3 elements!")
            # 4. Check that the first element of each header tuple is a string.        
            if not isinstance(header[0], str):
                raise ValueError("First element of each header tuple must be a string!")
            # 5. Check that the second element of each header tuple is a type.        
            if not isinstance(header[1], type):
                raise ValueError("Second element of each header tuple must be a Python type!")        
            # 6. Check that the third element of each header tuple is a valid variable ID.                
            if not self.is_id(header[2]) or not header.startswith("VR"):
                raise ValueError("Third element of each header tuple must be a valid variable ID!")
            
    def validate_class_column_names(self, class_column_names: dict):
        """Validate the class column names."""
        # TODO: Fix this, it should include type (int/str) and level (as a ResearchObject subclass of type "type")
        # 1. Check that the class column names are a dict.
        if not isinstance(class_column_names, dict):
            raise ValueError("Class column names must be a dict!")
        # 2. Check that the class column names are a dict of str to type.
        for key, value in class_column_names.items():
            if not isinstance(key, str):
                raise ValueError("Keys of class column names must be strings!")
            if value != int or value != str:
                raise ValueError("Values of class column names must be Python str or int types!")

    #################### Start Source objects ####################
    def get_analyses(self) -> list:
        """Return a list of analysis objects that belong to this logsheet."""
        from src.ResearchOS.PipelineObjects.analysis import Analysis
        an_ids = self._get_all_source_object_ids(cls = Analysis)
        return self._gen_obj_or_none(an_ids, Analysis)
    
    #################### Start Target objects ####################
    def get_variable_ids(self) -> list:
        """Return a list of variable IDs that belong to this logsheet."""
        from src.ResearchOS.variable import Variable
        vr_ids = self._get_all_target_object_ids(cls = Variable)
        return self._gen_obj_or_none(vr_ids, Variable)
    
    def add_variable_id(self, variable_id: str):
        """Add a variable to the logsheet."""
        # TODO: Mapping between variable ID and column header.
        from src.ResearchOS.variable import Variable
        self._add_target_object_id(variable_id, cls = Variable)

    def remove_variable_id(self, variable_id: str):
        """Remove a variable from the logsheet."""
        from src.ResearchOS.variable import Variable
        self._remove_target_object_id(variable_id, cls = Variable)

    #################### Start class-specific methods ####################
    def run_logsheet(self):
        """Run the logsheet import process."""
        from src.ResearchOS.PipelineObjects.project import Project
        from src.ResearchOS.DataObjects.dataset import Dataset
        from src.ResearchOS.user import User
        # 1. Validate that each attribute of the logsheet follows the proper format.
        self.validate_logsheet_headers(self.logsheet_headers)
        self.validate_class_column_names(self.class_column_names)
        self.validate_logsheet_path(self.logsheet_path)
        self.validate_num_header_rows(self.num_header_rows)

        pj = Project(id = User.get_current_project_id())
        ds = Dataset(id = pj.current_dataset_id)
        ds.validate_dataset_path(ds.dataset_path)
        ds.validate_data_schema(ds.data_schema)
        # 2. Load the logsheet

        # 3. TODO: Check that the length of logsheet headers list equals the number of columns in the logsheet.

        # 4. Run the logsheet import.

if __name__=="__main__":
    lg = Logsheet(id = "LG000000")
    lg = Logsheet(id = "LG000000_001")
    print(lg.num_header_rows)
    lg2 = lg.copy_to_new_instance()
    lg.num_header_rows = -1
    print(lg.num_header_rows)