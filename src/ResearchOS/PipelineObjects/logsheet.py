from ResearchOS import PipelineObject, DataObject, Variable
from ResearchOS.action import Action
import json

from abc import abstractmethod

# Defaults should be of the same type as the expected values.
default_instance_attrs = {}
default_instance_attrs["logsheet_path"] = None
default_instance_attrs["logsheet_headers"] = None
default_instance_attrs["num_header_rows"] = None
default_instance_attrs["class_column_names"] = None
default_abstract_attrs = {}

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
            attrs = default_instance_attrs  
        else:
            attrs = default_abstract_attrs    
        super().__init__(default_attrs = attrs, **kwargs)

    def __str__(self):
        if self.is_instance_object():
            return super().__str__(default_instance_attrs.keys(), self.__dict__)
        return super().__str__(default_abstract_attrs.keys(), self.__dict__)
    
    def __repr__(self) -> str:
        pass

    def from_json_logsheet_headers(self, json_var: list, action: Action) -> list:
        """Convert the attribute from JSON to the proper data type/format, if json.loads is not sufficient.
        XXX is the exact name of the attribute. Method name must follow this format."""        
        var = json.loads(None)
        return var
    
    def to_json_logsheet_headers(self, var: list, action: Action) -> list:
        """Convert the attribute from JSON to the proper data type/format, if json.loads is not sufficient.
        XXX is the exact name of the attribute. Method name must follow this format."""        
        json_var = json.dumps(None)
        return json_var

    def from_json_class_column_names(self, json_var: dict, action: Action) -> dict:
        """Convert the attribute from JSON to the proper data type/format, if json.loads is not sufficient.
        XXX is the exact name of the attribute. Method name must follow this format."""        
        var = None
        return var
    
    def to_json_class_column_names(self, var: dict, action: Action) -> dict:
        """Convert the attribute from JSON to the proper data type/format, if json.loads is not sufficient.
        XXX is the exact name of the attribute. Method name must follow this format."""        
        json_var = None
        return json_var

    def validate_num_header_rows(self, num_header_rows: int) -> None:
        """Validate the number of header rows. If it is not valid, the value is rejected."""        
        if not isinstance(num_header_rows, int):
            raise ValueError("Num header rows must be an integer!")
        if num_header_rows<0:
            raise ValueError("Num header rows must be positive!")
        if num_header_rows % 1 != 0:
            raise ValueError("Num header rows must be an integer!")
        
    def validate_logsheet_path(self, path: str) -> None:
        """Validate the logsheet path."""
        # 1. Check that the path exists in the file system.
        import os
        if not os.path.exists(path):
            raise ValueError("Specified path does not exist!")
        # 2. Check that the path is a file.
        if not os.path.isfile(path):
            raise ValueError("Specified path is not a file!")
        # 3. Check that the file is a CSV.
        if not path.endswith(("csv", "xlsx", "xls")):
            raise ValueError("Specified file is not a CSV!")
        # 4. Check that the file is not empty.
        if os.stat(path).st_size == 0:
            raise ValueError("Specified file is empty!")
        
    def validate_logsheet_headers(self, headers: list) -> None:
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
            if not self.is_id(header[2]) or not header.startswith(Variable.prefix):
                raise ValueError("Third element of each header tuple must be a valid variable ID!")
            
    def validate_class_column_names(self, class_column_names: dict) -> None:
        """Validate the class column names."""
        # TODO: Fix this, it should include type (int/str) and level (as a DataObject subclass of type "type")
        # 1. Check that the class column names are a dict.
        if not isinstance(class_column_names, dict):
            raise ValueError("Class column names must be a dict!")
        # 2. Check that the class column names are a dict of str to type.
        for key, value in class_column_names.items():
            if not isinstance(key, str):
                raise ValueError("Keys of class column names must be strings!")
            if not isinstance(value, DataObject):
                raise ValueError("Values of class column names must be Python types that subclass DataObject!")

    #################### Start Source objects ####################
    def get_analyses(self) -> list:
        """Return a list of analysis objects that belong to this logsheet."""
        from ResearchOS.PipelineObjects.analysis import Analysis
        an_ids = self._get_all_source_object_ids(cls = Analysis)
        return self._gen_obj_or_none(an_ids, Analysis)
    
    #################### Start Target objects ####################
    def get_variable_ids(self) -> list:
        """Return a list of variable IDs that belong to this logsheet."""
        from ResearchOS import Variable
        vr_ids = self._get_all_target_object_ids(cls = Variable)
        return self._gen_obj_or_none(vr_ids, Variable)
    
    def add_variable_id(self, variable_id: str) -> None:
        """Add a variable to the logsheet."""
        # TODO: Mapping between variable ID and column header.
        from ResearchOS import Variable
        self._add_target_object_id(variable_id, cls = Variable)

    def remove_variable_id(self, variable_id: str) -> None:
        """Remove a variable from the logsheet."""
        from ResearchOS import Variable
        self._remove_target_object_id(variable_id, cls = Variable)

    def validate_logsheet(self) -> None:
        """Run all validation methods to ensure that the logsheet is properly set up to be used."""
        from ResearchOS import Project, Dataset, User        
        # 1. Validate that each attribute of this logsheet and the other object types follows the proper format.
        self.validate_logsheet_headers(self.logsheet_headers)
        self.validate_class_column_names(self.class_column_names)
        self.validate_logsheet_path(self.logsheet_path)
        self.validate_num_header_rows(self.num_header_rows)

        us = User(id = User.get_current_user_object_id())
        pj = Project(id = us.current_project_id)
        us.validate_current_project_id(pj.id)
        ds = Dataset(id = pj.current_dataset_id)
        ds.validate_dataset_path(ds.dataset_path)
        ds.validate_schema(ds.schema)

        # 2. TODO: Check that the length of logsheet headers list equals the number of columns in the logsheet.

    #################### Start class-specific methods ####################
    def load_xlsx(self) -> list[list]:
        """Load the logsheet as a list of lists using Pandas."""
        import pandas as pd
        df = pd.read_excel(self.logsheet_path, header = None)
        return df.values.tolist()
    
    def read_logsheet(self) -> None:
        """Run the logsheet import process."""        
        # 1. Load the logsheet (using built-in Python libraries)
        if self.logsheet_path.endswith(("xlsx", "xls")):
            full_logsheet = self.load_xlsx()
        else:
            with open(self.logsheet_path, "r") as f:
                full_logsheet = f.readlines()

        # Run the logsheet import.
        headers = full_logsheet[0:self.num_header_rows]
        logsheet = full_logsheet[self.num_header_rows:]

        # Runtime checks.
        # a. Check that all of the logsheet headers match what is in the logsheet_headers attribute.

        # b. Check that all of the class column names match what is in the class_column_names attribute.
        # e.g. if there is a "Trial" level column, then there should be text in every trial row of that column.
        # at least one value per unique combination of class column names should be present in the logsheet.
        
        # For each row, connect instances of the appropriate DataObject subclass to all other instances of appropriate DataObject subclasses.
        for row in logsheet:
            dobjs = []
            for dobj in self.class_column_names:
                dobjs.append(dobj()) # Create a new instance of the DataObject subclass.
            # How do I know which data objects can connect to which others?
            for idx, cell in enumerate(row):
                header = self.logsheet_headers[idx]
                type_curr = header[1]
                # Convert the cell to the appropriate type.
                conv_header = type_curr(cell)
                # Get the Variable to assign the value to.
                var = Variable(id = header[2], parent = dobjs_instances[-1])
                # Assign the value to the Variable.



if __name__=="__main__":
    lg = Logsheet(id = "LG000000")
    lg = Logsheet(id = "LG000000_001")
    print(lg.num_header_rows)
    lg2 = lg.copy_to_new_instance()
    lg.num_header_rows = -1
    print(lg.num_header_rows)