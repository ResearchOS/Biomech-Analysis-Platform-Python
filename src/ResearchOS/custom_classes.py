import os
import json

import tomli as tomllib

from ResearchOS.constants import RAW_DATA_FOLDER_KEY, ENVIRON_VAR_DELIM, DATASET_SCHEMA_KEY

class Node():            

    def __str__(self):
        return f'{self.name}'
    
    def __repr__(self):
        return f'{self.name}'
    
    def __eq__(self, other):
        return self.name == other.name
    
    def __hash__(self):
        return hash(self.name)
    
    def set_attr_for_hashing(self, attrs_to_hash_no_data_object: list, attrs_to_hash_data_object: list, attrs: dict, data_object: list = None):
        attrs_to_hash_list = attrs_to_hash_data_object if data_object is not None else attrs_to_hash_no_data_object
        hash_attrs_dict = {k: v for k, v in attrs.items() if k in attrs_to_hash_list}
        self.attr_for_hashing = f"name:{self.name}::attrs:{json.dumps(hash_attrs_dict)}"

class Runnable(Node):

    def __init__(self, id: str, name: str, attrs: dict, data_object: str = None):        
        self.id = id
        self.name = name
        self.attrs = attrs
        attrs_to_hash_no_data_object = ['function', 'level', 'batch', 'subset']
        attrs_to_hash_data_object = ['function', 'level', 'batch']
        hash_attrs_list = attrs_to_hash_data_object if data_object is not None else attrs_to_hash_no_data_object
        hash_attrs_dict = {k: v for k, v in attrs.items() if k in hash_attrs_list}
        self.attr_for_hashing = f"name:{self.name}::attrs:{json.dumps(hash_attrs_dict)}"

class Logsheet(Runnable):

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)

        # Guaranteed to exist because of the earlier validation.
        self.outputs = attrs['outputs']   
    
class Process(Runnable):    

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)
        self.outputs = attrs['outputs']  

class Plot(Runnable):    

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)
        
class Stats(Runnable):    

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)


class Variable(Node):

    def __init__(self, id: str, name: str, attrs: str = {}, data_object: str = None):        
        self.id = id
        var_name = name.split('[')[0]
        self.name = var_name
        self.slices = []
        self.value = None
        if '[' in name:
            indices = name.split('[')[1:]
            self.slices = [index.replace(']', '') for index in indices]

        attrs["slices"] = self.slices
        if data_object is not None:
            attrs["resolved_value"] = self.value
        else:
            attrs["unresolved_value"] = self.value
        self.attrs = attrs

class Dynamic(Variable):
    """Abstract class for variables that are dynamic in some way."""
    pass

class InputVariable(Dynamic):
    """A fully defined input variable in the TOML file. This is a variable that receives a value from another runnable within the same package."""
    pass

class Unspecified(InputVariable):
    """Represents a variable that needs to be bridged to an output variable in another package.
    In TOML files, this is represented by "?".""" 
    pass   

class OutputVariable(Dynamic):
    """A variable that is directly outputted by a runnable."""    
    pass   

class LogsheetVariable(OutputVariable):   
    """A variable that is outputted by a logsheet."""    
    pass 

class Constant(InputVariable):
    """Directly hard-coded into the TOML file."""    

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)
        if type(self) == Constant:
            if 'value' not in attrs:
                raise ValueError(f'Constant {name} does not have a value.')
            self.value = attrs['value']
        elif 'value' not in attrs:
            self.value = None
        self.str_hash_value = json.dumps(self.value)

    def resolve(self, data_object: list):
        """Constants that are hard-coded into the TOML file do not need to be resolved."""
        pass    

class DataFilePath(Constant):
    """Data file path as an input variable to a runnable."""

    def resolve(self, data_object: list):
        """Get the data file path for the current data object."""
        ext = '.mat'
        save_data_folder = os.environ[RAW_DATA_FOLDER_KEY]
        relative_path = os.sep.join(data_object)
        self.value = os.path.join(save_data_folder, relative_path + ext)
        self.str_hash_value = self.value

class LoadConstantFromFile(Constant):
    """Constant that needs to be loaded from a file."""    

    def resolve(self, data_object: list):
        """Load the constant from the file."""
        file_name = self.value
        if not os.path.isabs(file_name):
            file_name = os.path.join(os.getcwd(), file_name)
        if file_name.endswith('.toml'):
            with open(file_name, 'rb') as f:
                self.value = tomllib.load(f)
        elif file_name.endswith('.json'):
            with open(file_name, 'rb') as f:
                self.value = json.load(f)
        else:
            raise ValueError(f'File type not supported: {file_name}.')
        self.str_hash_value = json.dumps(self.value)

class DataObjectName(Constant):
    """The name of a data object.""" 

    def resolve(self, data_object: list):
        """Return the specific data object name for the level provided."""
        # 1. Get the index of the data object name needed (e.g. "Trial" would return index 1 in the data object list ["Subject", "Trial"])
        dataset_schema = os.environ[DATASET_SCHEMA_KEY].split(ENVIRON_VAR_DELIM)
        index = dataset_schema.index(self.value)
        # 2. Extract the data object name from that index of the list
        self.value = data_object[index]  
        self.str_hash_value = self.value