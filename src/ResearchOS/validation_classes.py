import os

from ResearchOS.constants import PROCESS_NAME, PLOT_NAME, STATS_NAME, LOGSHEET_NAME


class RunnableFactory():    

    @classmethod    
    def create(cls, runnable_type: str):
        if runnable_type == PROCESS_NAME:
            runnable = ProcessType
        elif runnable_type == PLOT_NAME:
            runnable = PlotType
        elif runnable_type == STATS_NAME:
            runnable = StatsType
        elif runnable_type == LOGSHEET_NAME:
            runnable = LogsheetType
        return runnable    
    
def validate_inputs(inputs: dict):
    if not isinstance(inputs, dict):
        return False, "Inputs is not a dictionary."
    if len(inputs)==0:
        return False, "Inputs dictionary is empty."
    for key, value in inputs.items():
        if not isinstance(key, str):
            return False, "Input key is not a string."
        if key.count('.') > 0:
            return False, "Input key should not contain any periods."
    return True, None

def validate_outputs(outputs: list):
    if type(outputs) not in [list, str]:
        return False, "Outputs is not a list or string."
    if len(outputs)==0:
        return True, None
    if isinstance(outputs, list):
        if not all(isinstance(item, str) for item in outputs):
            return False, "Each item in outputs is not a string."
    else:
        outputs = [outputs]
    for output in outputs:
        if '.' in output:
            return False, "Output variable name cannot contain a period."
    return True, None

def validate_path(path: str):
    if not isinstance(path, str):
        return False, "Path is not a string."
    if len(path) == 0:
        return False, "Path is empty."
    if os.path.isabs(path):
        return False, "Path must be relative to the package's folder" # Path must be relative to the package's folder.
    return True, None

def validate_name(name: str):
    if not isinstance(name, str):
        return False, "Name is not a string."
    if len(name) == 0:
        return False, "Name is empty."
    return True, None

def validate_subset(subset: str):
    return True, None

def validate_level(level: str):
    if not (level is None or isinstance(level, str)):
        return False, "Level specified but is not a string."
    return True, None

def validate_batch(batch: str):
    allowable_types = [str, list, type(None)]
    if not type(batch) in allowable_types:
        return False, "Batch is not a string, list, or None."
    if isinstance(batch, list):
        if len(batch) > 1 and any([item is None for item in batch]):
            return False, "If batch is a list and contains None, then it must be of length 1"
        if not all(isinstance(item, (str, type(None))) for item in batch):
            return False, "Each item in batch is not a string or None." # Ensure each item of the list is a str, if it's a list.
    return True, None

def validate_language(language: str):
    if not isinstance(language, str):
        return False, "Language is not a string."
    
    if language.lower() not in ['matlab', 'python']:
        return False, "Language is not 'matlab' or 'python'."
    return True, None

def validate_num_header_rows(num_header_rows: int):
    if not isinstance(num_header_rows, int):
        return False, "num_header_rows is not an integer."
    if num_header_rows < 0:
        return False, "num_header_rows is negative."
    return True, None

def validate_class_column_names(class_column_names: dict):
    if not isinstance(class_column_names, dict):
        return False, "class_column_names is not a dictionary."
    if len(class_column_names)==0:
        return False, "class_column_names dictionary is empty."
    for key, value in class_column_names.items():
        if not isinstance(key, str):
            return False, "class_column_names key is not a string."
        if len(key) == 0:
            return False, "class_column_names key is empty."
        if not isinstance(value, str):
            return False, "class_column_names value is not a string."
    return True, None

def validate_headers(headers: dict):
    for key, value in headers.items():
        if not isinstance(value, list):
            return False, "Each header value should be a list."
        if len(value) != 2:
            return False, "Each header value should have two elements."
        if value[1] not in ['str','num','bool']:
            return False, "Each header value should specify if it is a string, number, or boolean."
        if not isinstance(value[0], str):
            return False, "Each header value should be a string."
    return True, None

def standardize_inputs(inputs: dict):
    new_inputs = {}
    for key, value in inputs.items():
        new_inputs[str(key).lower()] = value
    return new_inputs

def standardize_outputs(outputs: list):
    if len(outputs)==0:
        return [] # Works for string or list.
    if isinstance(outputs, str):
        outputs = [outputs]
    return [str(output).lower() for output in outputs]

def standardize_path(path: str):
    """Standardize the path by making it absolute for clarity."""
    package_folder = os.environ['PACKAGE_FOLDER']
    if not os.path.isabs(path):
        path = package_folder + os.sep + path
    return path

def standardize_name(name: str):
    return str(name).lower()

def standardize_subset(subset: str):
    return subset

def standardize_level(level: str):
    # Capitalize just the first letter of each level, i.e. "sentence case"
    if level is None:
        # If it exists, set level to the lowest Data Object in the dataset schema.
        if 'DATASET_SCHEMA' in os.environ:
            schema = os.environ['DATASET_SCHEMA']
            schema = schema.split('.')
            level = schema[-1]
        else:
            return level

    if len(level) > 1:
        return level[0].upper() + level[1:].lower()
    
    # Single letter. Not advised, but allowable.
    return level.upper()

def standardize_batch(batch: str):
    if batch is None:
        return [None]
    if isinstance(batch, str):
        batch = [batch]
    for index, b in enumerate(batch):
        if b is None:
            continue
        b = b.strip() # Remove leading and trailing whitespace.
        batch[index] = b[0].upper() + b[1:].lower() if len(b) > 1 else b.upper() # Sentence-case.
    return batch

def standardize_language(language: str):
    return str(language).strip().lower()

def standardize_num_header_rows(num_header_rows: int):
    return int(num_header_rows)

def standardize_class_column_names(class_column_names: dict):
    new_class_column_names = {}
    for key, value in class_column_names.items():
        key = key[0].upper() + key[1:].lower() if len(key) > 1 else key.upper()
        new_class_column_names[key] = value
    return new_class_column_names

def standardize_headers(headers: dict):
    new_headers = {}
    for key, value in headers.items():
        value[0] = value[0][0].upper() + value[0][1:].lower() if len(value[0]) > 1 else value[0].upper()
        value[1] = value[1].lower()
        key = key.strip() # Remove leading and trailing whitespace.
        new_headers[key] = value
    return new_headers


class RunnableType():
    """Specify the attributes needed for a Runnable to be properly added to the DAG during compilation.
    First is the minimum needed for compilation only "..._compilation". 
    Second is the minimum needed for running (after compilation) "..._running"."""
    runnable_minimum_required_manual_attrs_compilation = []
    runnable_attrs_fillable_w_defaults_compilation = {}

    runnable_minimum_required_manual_attrs_running = ['path']
    runnable_attrs_fillable_w_defaults_running = {'language': 'matlab'}

    @classmethod
    def validate(cls, attrs, compilation_only: bool):
        is_valid = True
        if attrs == {}:
            return True, None # Skip empty dictionaries
        
        # The minimum required attributes must be present
        missing_minimal_attrs = [attr for attr in cls.runnable_minimum_required_manual_attrs_compilation if attr not in attrs]
        if missing_minimal_attrs != []:
            return False, "Missing minimal attributes for compilation: " + str(missing_minimal_attrs)                            

        err_msg = []
        if not compilation_only:
            missing_minimal_attrs = [attr for attr in cls.runnable_minimum_required_manual_attrs_running if attr not in attrs]
            if missing_minimal_attrs != []:
                return False, "Missing minimal attributes for running: " + str(missing_minimal_attrs)
            is_valid_path, path_err_msg = validate_path(attrs['path'])
            is_valid_level, level_err_msg = validate_level(attrs['level'])
            is_valid_batch, batch_err_msg = validate_batch(attrs['batch'])
            is_valid_language, language_err_msg = validate_language(attrs['language'])
            is_valid = is_valid_path and is_valid_level and is_valid_batch and is_valid_language and is_valid
            err_msg = [msg for msg in [path_err_msg, level_err_msg, batch_err_msg, language_err_msg] if msg is not None]
        err_msg = '; '.join([msg for msg in err_msg if msg is not None])
        if not err_msg:
            err_msg = None
        return is_valid, err_msg

    @classmethod
    def standardize(cls, attrs, compilation_only: bool):
        if attrs == {}:
            return attrs # Skip empty dictionaries
        
        # Fill in the missing fillable attributes with their default values
        missing_fillable_attrs = [attr for attr in cls.runnable_attrs_fillable_w_defaults_compilation if attr not in attrs]
        for attr in missing_fillable_attrs:
            attrs[attr] = cls.runnable_attrs_fillable_w_defaults_compilation[attr]              
        
        if not compilation_only:
            missing_fillable_attrs_running = [attr for attr in cls.runnable_attrs_fillable_w_defaults_running if attr not in attrs]
            for attr in missing_fillable_attrs_running:
                attrs[attr] = cls.runnable_attrs_fillable_w_defaults_running[attr]
            attrs['level'] = standardize_level(attrs['level'])
            attrs['batch'] = standardize_batch(attrs['batch'])
            attrs['language'] = standardize_language(attrs['language'])

        return attrs
    
class ProcessType():
    minimum_required_manual_attrs_compilation = ['inputs','outputs'] # Don't include the attributes from the RunnableType() class
    attrs_fillable_w_defaults_compilation = {'batch': None, 'level': None}
    
    @classmethod
    def validate(cls, attrs, compilation_only: bool):
        is_valid, err_msg = RunnableType.validate(attrs, compilation_only) 
        if attrs == {}:
            return is_valid, err_msg       

        err_msg = [err_msg]
        missing_minimal_attrs = [attr for attr in cls.minimum_required_manual_attrs_compilation if attr not in attrs]
        if missing_minimal_attrs != []:
            return False, "Missing minimal attributes: " + str(missing_minimal_attrs)        
        
        is_valid_input, err_msg_input = validate_inputs(attrs['inputs'])
        is_valid_output, err_msg_output = validate_outputs(attrs['outputs'])
        is_valid_batch = True; err_msg_batch = None
        if 'batch' in attrs:
            is_valid_batch, err_msg_batch = validate_batch(attrs['batch'])        
        is_valid = is_valid and is_valid_output and is_valid_input and is_valid_batch
        err_msg = '; '.join([msg for msg in err_msg + [err_msg_output, err_msg_input, err_msg_batch] if msg is not None])
        return is_valid, err_msg

    @classmethod
    def standardize(cls, attrs, compilation_only: bool):
        attrs = RunnableType.standardize(attrs, compilation_only)
        if attrs == {}:
            return attrs
        
        for attr in cls.attrs_fillable_w_defaults_compilation:
            if attr not in attrs:
                attrs[attr] = cls.attrs_fillable_w_defaults_compilation[attr]

        attrs['inputs'] = standardize_inputs(attrs['inputs'])
        attrs['outputs'] = standardize_outputs(attrs['outputs'])   
        attrs['batch'] = standardize_batch(attrs['batch'])   

        if not compilation_only:
            attrs['subset'] = standardize_subset(attrs['subset'])
        return attrs

class PlotType():
    minimum_required_manual_attrs_compilation = ['inputs'] # Don't include the attributes from the RunnableType() class
    attrs_fillable_w_defaults_compilation = {'batch': None, 'level': None}

    @classmethod
    def validate(cls, attrs, compilation_only: bool):
        is_valid, err_msg = RunnableType.validate(attrs, compilation_only)
        if attrs == {}:
            return is_valid, err_msg
        err_msg = [err_msg]
        if not compilation_only:
            is_valid_subset, err_msg_subset = validate_subset(attrs['subset'])
            is_valid = is_valid and is_valid_subset
            err_msg = [msg for msg in err_msg + [err_msg_subset] if msg is not None]
        err_msg = '; '.join([msg for msg in err_msg if msg is not None])
        return is_valid, err_msg

    @classmethod
    def standardize(cls, attrs, compilation_only: bool):
        attrs = RunnableType.standardize(attrs, compilation_only)
        if attrs == {}:
            return attrs
        
        for attr in cls.attrs_fillable_w_defaults_compilation:
            if attr not in attrs:
                attrs[attr] = cls.attrs_fillable_w_defaults_compilation[attr]

        attrs['inputs'] = standardize_inputs(attrs['inputs'])
        attrs['batch'] = standardize_batch(attrs['batch'])
        if not compilation_only:
            attrs['subset'] = standardize_subset(attrs['subset'])
        return attrs

class StatsType():
    minimum_required_manual_attrs_compilation = ['inputs'] # Don't include the attributes from the RunnableType() class
    attrs_fillable_w_defaults_compilation = {'batch': None, 'level': None}

    @classmethod
    def validate(cls, attrs, compilation_only: bool):
        is_valid, err_msg = RunnableType.validate(attrs, compilation_only=compilation_only)
        if attrs == {}:
            return is_valid, err_msg
        err_msg = [err_msg]
        is_valid_batch, err_msg_batch = validate_batch(attrs['batch'])
        is_valid_level, err_msg_level = validate_level(attrs['level'])
        
        if not compilation_only:
            is_valid, err_msg = validate_subset(attrs['subset'])
        return is_valid, err_msg

    @classmethod
    def standardize(cls, attrs, compilation_only: bool):
        attrs = RunnableType.standardize(attrs, compilation_only=compilation_only)
        if attrs == {}:
            return attrs
        
        for attr in cls.attrs_fillable_w_defaults_compilation:
            if attr not in attrs:
                attrs[attr] = cls.attrs_fillable_w_defaults_compilation[attr]

        attrs['inputs'] = standardize_inputs(attrs['inputs'])
        attrs['batch'] = standardize_batch(attrs['batch'])
        if not compilation_only:
            attrs['subset'] = standardize_subset(attrs['subset'])
        return attrs

class LogsheetType():
    minimum_required_manual_attrs_compilation = ['outputs', 'headers', 'num_header_rows','class_column_names'] # Don't include the attributes from the RunnableType() class
    attrs_fillable_w_defaults_compilation = {}
    
    @classmethod
    def validate(cls, attrs, compilation_only: bool):
        is_valid, err_msg = RunnableType.validate(attrs, compilation_only=compilation_only)
        if attrs == {}:
            return is_valid, err_msg
        
        err_msg = [err_msg]
        
        is_valid_output, err_msg_output = validate_outputs(attrs['outputs'])
        is_valid_headers, err_msg_headers = validate_headers(attrs['headers'])
        is_valid_num_header_rows, err_msg_num_header_rows = validate_num_header_rows(attrs['num_header_rows'])
        is_valid_class_column_names, err_msg_class_column_names = validate_class_column_names(attrs['class_column_names'])
        is_valid = is_valid and is_valid_output and is_valid_headers and is_valid_num_header_rows and is_valid_class_column_names
        err_msg = '; '.join([msg for msg in err_msg + [err_msg_output, err_msg_headers, err_msg_num_header_rows, err_msg_class_column_names] if msg is not None])
        if not err_msg:
            err_msg = None
        return is_valid, err_msg

    @classmethod
    def standardize(cls, attrs, compilation_only: bool):
        attrs = RunnableType.standardize(attrs, compilation_only=compilation_only)
        if attrs == {}:
            return attrs
        
        attrs['outputs'] = standardize_outputs(attrs['outputs'])
        attrs['headers'] = standardize_headers(attrs['headers'])
        attrs['class_column_names'] = standardize_class_column_names(attrs['class_column_names'])
        attrs['num_header_rows'] = standardize_num_header_rows(attrs['num_header_rows'])
        return attrs