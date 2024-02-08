from typing import Any
import json

# import pandas as pd

import ResearchOS as ros
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.db_connection_factory import DBConnectionFactory

# Defaults should be of the same type as the expected values.
all_default_attrs = {}
all_default_attrs["path"] = None
all_default_attrs["headers"] = []
all_default_attrs["num_header_rows"] = None
all_default_attrs["class_column_names"] = {}
all_default_attrs["subset_id"] = None

complex_attrs_list = []

class Logsheet(ros.PipelineObject):

    prefix = "LG"

    #################### Start class-specific attributes ###################
    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        super().__init__(all_default_attrs, **kwargs)

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""
        if name == "vr":
            raise ValueError("The attribute 'vr' is not allowed to be set for Pipeline Objects.")
        else:
            ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)

    def load(self) -> None:
        """Load the dataset-specific attributes from the database in an attribute-specific way."""
        ros.PipelineObject.load(self) # Load the attributes specific to it being a PipelineObject.

    ### Logsheet path
        
    def validate_path(self, path: str) -> None:
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
        # if os.stat(path).st_size == 0:
        #     raise ValueError("Specified file is empty!")
        
    ### Logsheet headers
    
    def validate_headers(self, headers: list) -> None:
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
            if not ResearchObjectHandler.is_ro_id(header[2]) or not header.startswith(ros.Variable.prefix):
                raise ValueError("Third element of each header tuple must be a valid variable ID!")
            
    def to_json_headers(self, headers: list) -> str:
        """Convert the headers to a JSON string."""
        str_headers = []
        for header in headers:
            str_headers.append((header[0], header[1].prefix, header[2]))
        return json.dumps(str_headers)

    def from_json_headers(self, json_var: str) -> list:
        """Convert the JSON string to a list of headers."""
        str_var = json.loads(json_var)
        headers = []
        all_classes = ResearchObjectHandler._get_subclasses(ros.ResearchObject)
        for header in str_var:
            for cls in all_classes:
                if hasattr(cls, "prefix") and cls.prefix == header[1]:
                    headers.append((header[0], cls, header[2]))
                    break
        return headers
            
    ### Num header rows
            
    def validate_num_header_rows(self, num_header_rows: int) -> None:
        """Validate the number of header rows. If it is not valid, the value is rejected."""        
        if not isinstance(num_header_rows, int):
            raise ValueError("Num header rows must be an integer!")
        if num_header_rows<0:
            raise ValueError("Num header rows must be positive!")
        if num_header_rows % 1 != 0:
            raise ValueError("Num header rows must be an integer!")  
        
    ### Class column names

    def from_json_class_column_names(self, json_var: dict) -> dict:
        """Convert the dict from JSON string where values are class prefixes to a dict where keys are column names and values are DataObject subclasses."""     
        prefix_var = json.loads(json_var)
        class_column_names = {}
        all_classes = ResearchObjectHandler._get_subclasses(ros.ResearchObject)
        for key, prefix in prefix_var.items():
            for cls in all_classes:
                if hasattr(cls, "prefix") and cls.prefix == prefix:
                    class_column_names[key] = cls
                    break
        return class_column_names
    
    def to_json_class_column_names(self, var: dict) -> dict:
        """Convert the dict from a dict where keys are column names and values are DataObject subclasses to a JSON string where values are class prefixes."""        
        prefix_var = {}
        for key in var:
            prefix_var[key] = var[key].prefix
        return json.dumps(prefix_var)
            
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
            if not issubclass(value, ros.DataObject):
                raise ValueError("Values of class column names must be Python types that subclass DataObject!")
            
    ### Subset ID
            
    def validate_subset_id(self, subset_id: str) -> None:
        """Validate the subset ID."""
        if not ResearchObjectHandler.object_exists(subset_id):
            raise ValueError("Subset ID must be a valid ID!")
        if not subset_id.startswith(ros.Subset.prefix):
            raise ValueError("Subset ID must start with the correct prefix!")

    #################### Start class-specific methods ####################
    def load_xlsx(self) -> list[list]:
        """Load the logsheet as a list of lists using Pandas."""        
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