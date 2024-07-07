from ResearchOS.overhaul.constants import PROCESS_NAME, PLOT_NAME, STATS_NAME, UNSPECIFIED_VARIABLE_NAME, CONSTANT_VARIABLE_NAME, INPUT_VARIABLE_NAME, OUTPUT_VARIABLE_NAME, LOGSHEET_NAME
from ResearchOS.overhaul.constants import DATASET_KEY

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
        if 'path' not in attrs:            
            raise ValueError(f'Runnable {name} does not have a path.')
        if 'inputs' not in attrs:
            raise ValueError(f'Runnable {name} does not have inputs.')
        if 'subset' not in attrs:
            raise ValueError(f'Runnable {name} does not have a subset.')
        if 'level' not in attrs:
            attrs['level'] = None # Takes on the value of the lowest level data object type, e.g. "Trial"
        if 'batch' not in attrs:
            attrs['batch'] = [attrs['level']] # Takes on the value of attrs['level']

        self.path = attrs['path']
        self.inputs = attrs['inputs']        
        self.subset = attrs['subset']  
        self.level = attrs['level']
        self.batch = attrs['batch']

class Logsheet(Runnable):
    class_name = LOGSHEET_NAME

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)
        attrs['path'] = None
        attrs['level'] = DATASET_KEY
        attrs['subset'] = None
        attrs['inputs'] = None
        if 'outputs' not in attrs:
            raise ValueError(f'Logsheet {name} does not have outputs.')
        self.outputs = attrs['outputs']        
    
class Process(Runnable):
    class_name = PROCESS_NAME

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)
        if 'outputs' not in attrs:
            attrs['outputs'] = None
        self.outputs = attrs['outputs']        

class Plot(Runnable):
    class_name = PLOT_NAME

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)
        
class Stats(Runnable):
    class_name = STATS_NAME

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)


class Variable(Node):

    def __init__(self, id: str, name: str, attrs: str = {}):
        self.id = id
        self.name = name

class Dynamic(Variable):
    pass

class InputVariable(Dynamic):
    class_name = INPUT_VARIABLE_NAME

class Unspecified(InputVariable):
    class_name = UNSPECIFIED_VARIABLE_NAME

class OutputVariable(Dynamic):
    class_name = OUTPUT_VARIABLE_NAME

class LogsheetVariable(OutputVariable):
    class_name = "logsheet_variable"

class Constant(Variable):
    """Directly hard-coded into the TOML file."""
    class_name = CONSTANT_VARIABLE_NAME

    def __init__(self, id: str, name: str, attrs: str):
        super().__init__(id, name, attrs)
        if type(self) == Constant:
            if 'value' not in attrs:
                raise ValueError(f'Constant {name} does not have a value.')
            self.value = attrs['value']

class DataFilePath(Constant):
    class_name = "data_file_path"

class LoadConstantFromFile(Constant):
    class_name = "load_constant_from_file"

class DataObjectName(Variable):
    class_name = "data_object_name"