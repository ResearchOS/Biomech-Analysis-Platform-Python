from abc import abstractmethod, ABC

from ResearchOS.constants import PROCESS_NAME, PLOT_NAME, STATS_NAME, UNSPECIFIED_VARIABLE_NAME, CONSTANT_VARIABLE_NAME, INPUT_VARIABLE_NAME, OUTPUT_VARIABLE_NAME, LOGSHEET_NAME
from ResearchOS.constants import DATASET_KEY, DATA_OBJECT_NAME_KEY

class Node():

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
        self.id = id
        self.name = name
        self.attrs = attrs

class Logsheet(Runnable):    

    def __init__(self, id: str, name: str, attrs: str):
        attrs['path'] = None
        attrs['level'] = DATASET_KEY        
        attrs['subset'] = None
        attrs['inputs'] = None
        super().__init__(id, name, attrs)
        if 'outputs' not in attrs:
            raise ValueError(f'Logsheet {name} does not have outputs.')
        self.outputs = attrs['outputs']   

    def validate(self, attrs):
        pass
    
class Process(Runnable):    

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)
        if 'outputs' not in attrs:
            attrs['outputs'] = None
        self.outputs = attrs['outputs']  

class Plot(Runnable):    

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)
        
class Stats(Runnable):    

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)


class Variable(Node):

    def __init__(self, id: str, name: str, attrs: str = {}):
        self.id = id
        self.name = name

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

class Constant(Variable):
    """Directly hard-coded into the TOML file."""    

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)
        if type(self) == Constant:
            if 'value' not in attrs:
                raise ValueError(f'Constant {name} does not have a value.')
            self.value = attrs['value']

class DataFilePath(Constant):
    """Data file path as an input variable to a runnable."""
    pass   

class LoadConstantFromFile(Constant):
    """Constant that needs to be loaded from a file."""    
    pass   

class DataObjectName(Variable):
    """The name of a data object.""" 
    pass   