PACKAGES_PREFIX = 'ros-'

### paths.toml
LOGSHEET_PATH_KEY = 'logsheet_path'
MAT_DATA_FOLDER_KEY = 'mat_data_folder'
RAW_DATA_FOLDER_KEY = 'raw_data_folder'

### project_settings.toml
DATASET_SCHEMA_KEY = 'dataset_schema' # e.g. "Subject1.Condition1.Trial1"
DATASET_FILE_SCHEMA_KEY = 'dataset_file_schema' # e.g. "Subject1.Trial1"
DATASET_KEY = 'dataset'

### In bridges.toml only
# Special strings
LOGSHEET_VAR_KEY = '__logsheet__' # i.e. "__logsheet__.var_name"
SOURCES_KEY = 'sources' # for dynamic input variables, this is the "package.function.var_name" of the source variable. For constants, this is the value.
TARGETS_KEY = 'targets' # the "package.function.var_name" of the target variable
# Keys allowed in the index.TOML files.
PROCESS_NAME = 'process'
PLOT_NAME = 'plot'
STATS_NAME = 'stats'
BRIDGES_KEY = 'bridges'
PACKAGE_SETTINGS_KEY = 'project_settings'
SUBSET_KEY = 'subsets'

### In each package's runnables TOML files.
# Special strings
DATA_OBJECT_NAME_STR = '__name__' # Returns the short name of the data object, e.g. "Trial1"
DATA_OBJECT_FULL_NAME_STR = '__full_name__' # Returns the full name of the data object, e.g. "Subject1.Condition1.Trial1"
PROJECT_FOLDER_STR = '__project_folder__' # Returns the project folder
# Keys in input variable dictionary.
LOAD_CONSTANT_FROM_FILE_KEY = '__load_file__' # Load the constant from the specified file, {__load_file__: file_path}
DATA_FILE_KEY = '__file_path__' # Use the absolute path of the current data object's data file as input (minus the file extension) {__file_path__: file_path}

### Custom node class names.
INPUT_VARIABLE_NAME = 'input'
OUTPUT_VARIABLE_NAME = 'output'
CONSTANT_VARIABLE_NAME = 'constant'
UNSPECIFIED_VARIABLE_NAME = 'unspecified'
LOGSHEET_NAME = 'logsheet'
DATA_OBJECT_NAME_KEY = 'data_object_name'
