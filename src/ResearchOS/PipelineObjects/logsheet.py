from typing import Any
import json, csv

# import pandas as pd
from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.variable import Variable
from ResearchOS.research_object import ResearchObject
from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.PipelineObjects.subset import Subset
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

class Logsheet(PipelineObject):

    prefix = "LG"

    #################### Start class-specific attributes ###################
    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        super().__init__(all_default_attrs, **kwargs)

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""
        ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)

    def load(self) -> None:
        """Load the dataset-specific attributes from the database in an attribute-specific way."""
        PipelineObject.load(self) # Load the attributes specific to it being a PipelineObject.

    def read_and_clean_logsheet(self, nrows: int = None) -> None:
        """Read the logsheet (CSV only) and clean it."""
        logsheet = []
        first_elem_prefix = '\ufeff'
        with open(self.path, "r") as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')            

            for row_num, row in enumerate(reader):                                    
                logsheet.append(row)
                if nrows is not None and row_num == nrows-1:
                    break
        
        # 7. Check that the headers all match the logsheet.
        logsheet[0][0] = logsheet[0][0].replace(first_elem_prefix, "")
        return logsheet

    ### Logsheet path
        
    def validate_path(self, path: str) -> None:
        """Validate the logsheet path."""
        # 1. Check that the path exists in the file system.
        import os
        if not isinstance(path, str):
            raise ValueError("Path must be a string!")
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
        """Validate the logsheet headers. These are the headers that are in the logsheet file.
        The headers must be a list of tuples, where each tuple has 3 elements:
        1. A string (the name of the header)
        2. A type (the type of the header)
        3. A valid variable ID (the ID of the Variable that the header corresponds to)"""
        self.validate_path(self.path)

        # 1. Check that the headers are a list.
        if not isinstance(headers, list):
            raise ValueError("Headers must be a list!")
        
        # 2. Check that the headers are a list of tuples and meet the other requirements.        
        for header in headers:
            if not isinstance(header, tuple):
                raise ValueError("Headers must be a list of tuples!")
            # 3. Check that each header tuple has 3 elements.        
            if len(header) != 4:
                raise ValueError("Each header tuple must have 3 elements!")
            # 4. Check that the first element of each header tuple is a string.        
            if not isinstance(header[0], str):
                raise ValueError("First element of each header tuple must be a string!")
            # 5. Check that the second element of each header tuple is a type.        
            # if not isinstance(header[1], type):
            #     raise ValueError("Second element of each header tuple must be a Python type!")
            if header[1] not in [str, int, float]:
                raise ValueError("Second element of each header tuple must be a Python type!")
            if header[2] not in DataObject.__subclasses__():
                raise ValueError("Third element of each header tuple must be a ResearchObject subclass!")
            # 6. Check that the third element of each header tuple is a valid variable ID.                
            if not header[3].startswith(Variable.prefix) or not ResearchObjectHandler.object_exists(header[3]):
                raise ValueError("Third element of each header tuple must be a valid pre-existing variable ID!")
            
        logsheet = self.read_and_clean_logsheet(nrows = 1)
        headers_in_logsheet = logsheet[0]
        header_names = [header[0] for header in headers]
        missing = [header for header in headers_in_logsheet if header not in header_names]

        if len(missing) > 0:
            raise ValueError(f"The headers {missing} do not match between logsheet and code!")
            
    def to_json_headers(self, headers: list) -> str:
        """Convert the headers to a JSON string."""
        str_headers = []
        for header in headers:
            # Update the Variable object with the name if it is not already set, and the level.
            vr = Variable(id = header[3])
            if vr.name is None:
                vr.name = header[0]
            vr.level = header[2]
            str_headers.append((header[0], str(header[1])[8:-2], header[2].prefix, header[3]))
        return json.dumps(str_headers)

    def from_json_headers(self, json_var: str) -> list:
        """Convert the JSON string to a list of headers."""
        subclasses = DataObject.__subclasses__()
        str_var = json.loads(json_var)
        headers = []
        mapping = {
            "str": str,
            "int": int,
            "float": float
        }
        for header in str_var:
            cls_header = [cls for cls in subclasses if cls.prefix == header[2]][0]
            headers.append((header[0], mapping[header[1]], cls_header, header[3]))                
        return headers
            
    ### Num header rows
            
    def validate_num_header_rows(self, num_header_rows: int) -> None:
        """Validate the number of header rows. If it is not valid, the value is rejected."""                
        if not isinstance(num_header_rows, int | float):
            raise ValueError("Num header rows must be numeric!")
        if num_header_rows<0:
            raise ValueError("Num header rows must be positive!")
        if num_header_rows % 1 != 0:
            raise ValueError("Num header rows must be an integer!")  
        
    ### Class column names
        
    def validate_class_column_names(self, class_column_names: dict) -> None:
        """Validate the class column names. Must be a dict where the keys are the column names in the logsheet and the values are the DataObject subclasses."""
        # 1. Check that the class column names are a dict.
        if not isinstance(class_column_names, dict):
            raise ValueError("Class column names must be a dict!")
        # 2. Check that the class column names are a dict of str to type.        
        for key, value in class_column_names.items():
            if not isinstance(key, str):
                raise ValueError("Keys of class column names must be strings!")
            if not issubclass(value, DataObject):
                raise ValueError("Values of class column names must be Python types that subclass DataObject!")
            
        headers = self.read_and_clean_logsheet(nrows = 1)[0]
        if not all([header in headers for header in class_column_names.keys()]):
            raise ValueError("The class column names must be in the logsheet headers!")

    def from_json_class_column_names(self, json_var: dict) -> dict:
        """Convert the dict from JSON string where values are class prefixes to a dict where keys are column names and values are DataObject subclasses."""     
        prefix_var = json.loads(json_var)
        class_column_names = {}
        all_classes = ResearchObjectHandler._get_subclasses(ResearchObject)
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
            
    ### Subset ID
            
    def validate_subset_id(self, subset_id: str) -> None:
        """Validate the subset ID."""
        if not ResearchObjectHandler.object_exists(subset_id):
            raise ValueError("Subset ID must be a valid ID!")
        if not subset_id.startswith(Subset.prefix):
            raise ValueError("Subset ID must start with the correct prefix!")

    #################### Start class-specific methods ####################
    def load_xlsx(self) -> list[list]:
        """Load the logsheet as a list of lists using Pandas."""        
        df = pd.read_excel(self.path, header = None)
        return df.values.tolist()
    
    def read_logsheet(self) -> None:
        """Run the logsheet import process."""
        self.validate_class_column_names(self.class_column_names)
        self.validate_headers(self.headers)
        self.validate_num_header_rows(self.num_header_rows)
        self.validate_path(self.path)
        self.validate_subset_id(self.subset_id)

        conn = DBConnectionFactory.create_db_connection().conn

        # 1. Load the logsheet (using built-in Python libraries)
        if self.path.endswith(("xlsx", "xls")):
            full_logsheet = self.load_xlsx()
        else:
            full_logsheet = self.read_and_clean_logsheet()

        if len(full_logsheet) < self.num_header_rows:
            raise ValueError("The number of header rows is greater than the number of rows in the logsheet!")

        # Run the logsheet import.
        # headers = full_logsheet[0:self.num_header_rows]
        if len(full_logsheet) == self.num_header_rows:
            logsheet = []
        else:
            logsheet = full_logsheet[self.num_header_rows:]
        
        # For each row, connect instances of the appropriate DataObject subclass to all other instances of appropriate DataObject subclasses.
        headers_in_logsheet = full_logsheet[0]
        header_names = [header[0] for header in self.headers]
        header_types = [header[1] for header in self.headers]
        header_levels = [header[2] for header in self.headers]
        header_vrids = [header[3] for header in self.headers]
        # Load/create all of the Variables
        vr_list = []
        vr_obj_list = []
        for vr_id in header_vrids:
            vr = Variable(id = vr_id)
            vr_obj_list.append(vr)
            vr_list.append(vr.id)
        # Order the class column names by precedence in the schema so that higher level objects always exist before lower level, so they can be attached.
        # schema_ordered_col_names = self.order_class_column_names()
        idcreator = IDCreator(conn)
        for row in logsheet:            
            # Create a new instance of the appropriate DataObject subclass(es) and store it in the database.
            # TODO: How to order the data objects?
            for header, cls in self.class_column_names.items():
                col_idx = headers_in_logsheet.index(header)
                raw_value = row[col_idx]                
                value = header_types[col_idx](raw_value)
                level = header_levels[col_idx]                
                vr = vr_obj_list[col_idx]

                # Create the DataObject instance.
                new_dobj = cls(id = idcreator.create_ro_id(cls))

                if level is cls:
                    new_dobj.__dict__[vr.name] = vr

                # Attach the DataObject instance to its parent DataObject instance.



if __name__=="__main__":
    lg = Logsheet(id = "LG000000")
    lg = Logsheet(id = "LG000000_001")
    print(lg.num_header_rows)
    lg2 = lg.copy_to_new_instance()
    lg.num_header_rows = -1
    print(lg.num_header_rows)