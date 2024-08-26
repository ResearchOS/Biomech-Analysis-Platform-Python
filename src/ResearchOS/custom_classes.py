import os
import json

import tomli as tomllib

from ResearchOS.constants import RAW_DATA_FOLDER_KEY, ENVIRON_VAR_DELIM, DATASET_SCHEMA_KEY
from ResearchOS.run import get_node_settings_for_hash, get_node_settings

class Node():

    def __init__(self, id: str, name: str, attrs: dict):
        self.serialized = ""

    def __str__(self):
        return f'{self.name}'
    
    def __repr__(self):
        return f'{self.name}'
    
    def __eq__(self, other):
        return self.name == other.name
    
    def __hash__(self):
        return hash(self.name)

class Runnable(Node):

    def __init__(self, id: str, name: str, attrs: dict):
        super().__init__(id, name, attrs)
        self.id = id
        self.name = name
        self.attrs = attrs  

    def serialize_node(self):
        """Get the hash value of the constant."""
        node_settings = get_node_settings(self)
        node_settings_for_hash = get_node_settings_for_hash(node_settings)
        self.serialized = json.dumps(node_settings_for_hash)

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

    def __init__(self, id: str, name: str, attrs: str = {}):
        super().__init__(id, name, attrs)
        self.id = id
        var_name = name.split('[')[0]
        self.name = var_name
        self.slices = []
        self.value = None
        if '[' in name:
            indices = name.split('[')[1:]
            self.slices = [index.replace(']', '') for index in indices] 

    def serialize_node(self):
        """Get the hash value of the constant."""        
        self.serialized = json.dumps(self.value) if self.value is not None else ""

class Dynamic(Variable):
    """Abstract class for variables that are dynamic in some way."""
    pass

class InputVariable(Dynamic):
    """A fully defined input variable in the TOML file. This is a variable that receives a value from another runnable within the same package."""
    
    def serialize_node(self):
        """Get the hash value of the constant."""
        # TODO: Handle slices, that has to be included.
        if not self.slices:
            super().serialize_node()
        else:
            slices_str = ''.join(self.slices)
            self.serialized = json.dumps(self.value + slices_str) if self.value is not None else ""

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