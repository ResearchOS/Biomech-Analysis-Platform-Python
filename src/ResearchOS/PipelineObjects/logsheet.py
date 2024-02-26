from typing import Any
import json, csv, platform

import networkx as nx

# import pandas as pd
from ResearchOS.DataObjects.data_object import DataObject
from ResearchOS.variable import Variable
from ResearchOS.DataObjects.dataset import Dataset
from ResearchOS.research_object import ResearchObject
from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator

# Defaults should be of the same type as the expected values.
all_default_attrs = {}
all_default_attrs["path"] = None
all_default_attrs["headers"] = []
all_default_attrs["num_header_rows"] = None
all_default_attrs["class_column_names"] = {}

complex_attrs_list = []

class Logsheet(PipelineObject):

    prefix = "LG"    

    ### Logsheet path
        
    def validate_path(self, path: str, action: Action) -> None:
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
        
    ### Logsheet headers
    
    def validate_headers(self, headers: list, action: Action) -> None:
        """Validate the logsheet headers. These are the headers that are in the logsheet file.
        The headers must be a list of tuples, where each tuple has 3 elements:
        1. A string (the name of the header)
        2. A type (the type of the header)
        3. A valid variable ID (the ID of the Variable that the header corresponds to)"""
        self.validate_path(self.path, action)

        # 1. Check that the headers are a list.
        if not isinstance(headers, list):
            raise ValueError("Headers must be a list!")
        
        # 2. Check that the headers are a list of tuples and meet the other requirements.        
        for header in headers:
            if not isinstance(header, tuple):
                raise ValueError("Headers must be a list of tuples!")
            # 3. Check that each header tuple has 3 elements.        
            if len(header) != 4:
                raise ValueError("Each header tuple must have 4 elements!")
            # 4. Check that the first element of each header tuple is a string.        
            if not isinstance(header[0], str):
                raise ValueError("First element of each header tuple must be a string!")
            if header[1] not in [str, int, float]:
                raise ValueError("Second element of each header tuple must be a Python type!")
            if header[2] not in DataObject.__subclasses__():
                raise ValueError("Third element of each header tuple must be a ResearchObject subclass!")
            # 6. Check that the third element of each header tuple is a valid variable ID.                
            if not header[3].startswith(Variable.prefix) or not ResearchObjectHandler.object_exists(header[3], action):
                raise ValueError("Fourth element of each header tuple must be a valid pre-existing variable ID!")
            
        logsheet = self.read_and_clean_logsheet(nrows = 1)
        headers_in_logsheet = logsheet[0]
        header_names = [header[0] for header in headers]
        missing = [header for header in headers_in_logsheet if header not in header_names]

        if len(missing) > 0:
            raise ValueError(f"The headers {missing} do not match between logsheet and code!")
            
    def to_json_headers(self, headers: list, action: Action) -> str:
        """Convert the headers to a JSON string.
        Also sets the VR's name and level."""
        str_headers = []
        for header in headers:
            # Update the Variable object with the name if it is not already set, and the level.
            vr = Variable(id = header[3])
            vr.__setattr__("name", header[0], action = action)
            vr.__setattr__("level", header[2], action = action)
            str_headers.append((header[0], str(header[1])[8:-2], header[2].prefix, header[3]))
        return json.dumps(str_headers)

    def from_json_headers(self, json_var: str, action: Action) -> list:
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
            
    def validate_num_header_rows(self, num_header_rows: int, action: Action) -> None:
        """Validate the number of header rows. If it is not valid, the value is rejected."""                
        if not isinstance(num_header_rows, (int, float)):
            raise ValueError("Num header rows must be numeric!")
        if num_header_rows<0:
            raise ValueError("Num header rows must be positive!")
        if num_header_rows % 1 != 0:
            raise ValueError("Num header rows must be an integer!")  
        
    ### Class column names
        
    def validate_class_column_names(self, class_column_names: dict, action: Action) -> None:
        """Validate the class column names. Must be a dict where the keys are the column names in the logsheet and the values are the DataObject subclasses."""
        self.validate_path(self.path, action)
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

    def from_json_class_column_names(self, json_var: dict, action: Action) -> dict:
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
    
    def to_json_class_column_names(self, var: dict, action: Action) -> dict:
        """Convert the dict from a dict where keys are column names and values are DataObject subclasses to a JSON string where values are class prefixes."""        
        prefix_var = {}
        for key in var:
            prefix_var[key] = var[key].prefix
        return json.dumps(prefix_var)

    #################### Start class-specific methods ####################
    def read_and_clean_logsheet(self, nrows: int = None) -> list:
        """Read the logsheet (CSV only) and clean it."""
        logsheet = []
        if platform.system() == "Windows":
            first_elem_prefix = "ï»¿"
        else:
            first_elem_prefix = '\ufeff'
        with open(self.path, "r") as f:
            reader = csv.reader(f, delimiter=',', quotechar='"')            

            for row_num, row in enumerate(reader):                                    
                logsheet.append(row)
                if nrows is not None and row_num == nrows-1:
                    break
        
        # 7. Check that the headers all match the logsheet.
        logsheet[0][0] = logsheet[0][0][len(first_elem_prefix):]
        return logsheet
    
    def load_xlsx(self) -> list:
        """Load the logsheet as a list of lists using Pandas."""        
        df = pd.read_excel(self.path, header = None)
        return df.values.tolist()
    
    def read_logsheet(self) -> None:
        """Run the logsheet import process."""
        ds = Dataset(id = self.get_dataset_id())
        action = Action(name = "read logsheet")
        self.validate_class_column_names(self.class_column_names, action)
        self.validate_headers(self.headers, action)
        self.validate_num_header_rows(self.num_header_rows, action)
        self.validate_path(self.path, action)

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

        # logsheet = logsheet[0:50] # For testing purposes, only read the first 50 rows.
        
        # For each row, connect instances of the appropriate DataObject subclass to all other instances of appropriate DataObject subclasses.
        headers_in_logsheet = full_logsheet[0]
        all_headers = self.headers
        header_names = [header[0] for header in self.headers]
        header_types = [header[1] for header in self.headers]
        header_levels = [header[2] for header in self.headers]
        header_vrids = [header[3] for header in self.headers]
        # Load/create all of the Variables
        vr_list = []
        vr_obj_list = []
        for idx, vr_id in enumerate(header_vrids):
            vr = Variable(id = vr_id)
            assert vr.level == header_levels[idx]
            vr_obj_list.append(vr)
            vr_list.append(vr.id)
            
        # Order the class column names by precedence in the schema so that higher level objects always exist before lower level.
        schema = ds.schema
        schema_graph = nx.DiGraph()
        schema_graph.add_edges_from(schema)
        order = list(nx.topological_sort(schema_graph))
        if len(order) <= 1:
            raise ValueError("The schema must have at least 2 elements including the Dataset!")
        order = order[1:] # Remove the Dataset class from the order.
        dobj_column_names = []
        for cls in order:
            for column_name, cls_item in self.class_column_names.items():
                if cls is cls_item:
                    dobj_column_names.append(column_name)

        # Create the data objects.
        # Get all of the names of the data objects, after they're cleaned for SQLite.
        cols_idx = [headers_in_logsheet.index(header) for header in dobj_column_names] # Get the indices of the data objects columns.
        dobj_names = [] # The matrix of data object names (values in the logsheet).
        for row in logsheet:
            dobj_names.append([])
            for idx in cols_idx:
                raw_value = row[idx]
                type_class = header_types[idx]
                value = self.clean_value(type_class, raw_value)
                dobj_names[-1].append(value)
        for row_num, row in enumerate(dobj_names):
            if not all([str(cell).isidentifier() for cell in row]):
                raise ValueError(f"Row # (1-based): {row_num+self.num_header_rows+1} all data object names must be non-empty and valid variable names!")
        [row.insert(0, ds.id) for row in dobj_names] # Prepend the Dataset to the first column of each row.
        name_ids_dict = {} # The dict that maps the values (names) to the IDs. Separate dict for each class, each class is a top-level key of the dict.
        name_ids_dict[Dataset] = {ds.name: ds.id}
        name_dobjs_dict = {}
        name_dobjs_dict[Dataset] = {ds.name: ds}
        for cls in order:
            name_ids_dict[cls] = {} # Initialize the dict for this class.            
            name_dobjs_dict[cls] = {}

        # Create the DataObject instances in the dict.        
        all_dobjs_ordered = [] # The list of lists of DataObject instances, ordered by the order of the schema.                
        for row_num, row in enumerate(dobj_names):
            row = row[1:]
            all_dobjs_ordered.append([ds]) # Add the Dataset to the beginning of each row.
            for idx in range(len(row)):
                cls = order[idx] # The class to create.
                col_idx = cols_idx[idx] # The index of the column in the logsheet.
                value = self.clean_value(header_types[col_idx], row[idx])
                if value not in name_ids_dict[cls]:                    
                    name_ids_dict[cls][value] = IDCreator(action.conn).create_ro_id(cls)
                    dobj = cls(id = name_ids_dict[cls][value], name = value) # Create the research object.                
                    name_dobjs_dict[cls][value] = dobj
                    print("Creating DataObject, Row: ", row_num, "Column: ", cls.prefix, "Value: ", value, "ID: ", dobj.id)
                dobj = name_dobjs_dict[cls][value]
                all_dobjs_ordered[-1].append(dobj) # Matrix of all research objects.                

        # Arrange the address ID's that were generated into an edge list.
        # Then assign that to the Dataset.
        addresses = []
        for row in all_dobjs_ordered:
            for idx, dobj in enumerate(row):
                if idx == 0:
                    continue
                ids = [row[idx-1].id, dobj.id]
                if ids not in addresses:
                    addresses.append(ids)
        ds.addresses = addresses # Store addresses, also creates address_graph.
                
        
        # Assign the values to the DataObject instances.
        # Validates that the logsheet is of valid format.
        # i.e. Doesn't have conflicting values for one level (empty/None is OK)                        
        attrs_cache_dict = {}
        for row_num, row in enumerate(logsheet):
            row_dobjs = all_dobjs_ordered[row_num][1:]

            # Assign all of the data to the appropriate DataObject instances.
            # Includes the "data object columns" so that the DataObjects have an attribute with the name of the header name.
            for header in all_headers:
                name = header[0]
                col_idx = headers_in_logsheet.index(name)                
                type_class = header[1]
                level = header[2]
                level_idx = order.index(level)
                vr_id = header[3]                
                value = self.clean_value(type_class, row[headers_in_logsheet.index(name)])
                # Set up the cache dict for this data object.
                if not row_dobjs[level_idx].id in attrs_cache_dict:
                    attrs_cache_dict[row_dobjs[level_idx].id] = {}
                # Set up the cache dict for this data object for this attribute.
                if name not in attrs_cache_dict[row_dobjs[level_idx].id]:
                    attrs_cache_dict[row_dobjs[level_idx].id][name] = None
                print("Row: ", row_num, "Column: ", name, "Value: ", value)
                prev_value = attrs_cache_dict[row_dobjs[level_idx].id][name]
                # prev_value = getattr(row_dobjs[level_idx], name, None) # May not exist yet.                
                if prev_value is not None:                    
                    if prev_value == value or value is None:
                        continue
                    raise ValueError(f"Row # (1-based): {row_num+self.num_header_rows+1} Column: {name} has conflicting values!")
                row_dobjs[level_idx].__setattr__(name, value, action = action) # Set the attribute of this DataObject instance to the value in the logsheet.                
                attrs_cache_dict[row_dobjs[level_idx].id][name] = value
                dobj = row_dobjs[level_idx]
        action.execute()

    def clean_value(self, type_class: type, raw_value: Any) -> Any:
        """Convert to proper type and clean the value of the logsheet cell."""
        try:
            value = type_class(raw_value)
        except ValueError:
            value = raw_value
        if isinstance(value, str):
            value = value.replace("'", "''") # Handle single quotes.
        if value == '': # Empty
            value = None
        return value



if __name__=="__main__":
    lg = Logsheet(id = "LG000000")
    lg = Logsheet(id = "LG000000_001")
    print(lg.num_header_rows)
    lg2 = lg.copy_to_new_instance()
    lg.num_header_rows = -1
    print(lg.num_header_rows)