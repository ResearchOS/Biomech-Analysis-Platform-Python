PACKAGES_PREFIX = 'ros-'

### In each package's functions' TOML files.
# Special strings
DATA_OBJECT_NAME_KEY = '__name__'
PROJECT_FOLDER_KEY = '__project_folder__'

# Keys in input variable dictionary.
LOAD_CONSTANT_FROM_FILE_KEY = '__file__' # Load the constant from a file
DATA_FILE_KEY = '__file_path__' # Return the absolute path of the data file (minus the file extension)

### In bridges.toml only
# Special strings
LOGSHEET_KEY = '__logsheet__'
SOURCES_KEY = 'sources'
TARGETS_KEY = 'targets'

# Access project config variables
MAT_DATA_FOLDER_KEY = 'mat_data_folder'
RAW_DATA_FOLDER_KEY = 'raw_data_folder'

# Keys allowed in the TOML files.
PROCESS_NAME = 'process'
PLOT_NAME = 'plot'
STATS_NAME = 'stats'
BRIDGES_KEY = 'bridges'
PACKAGE_SETTINGS_KEY = 'package-settings'
SUBSET_KEY = 'subsets'

INPUT_VARIABLE_NAME = 'input'
OUTPUT_VARIABLE_NAME = 'output'
CONSTANT_VARIABLE_NAME = 'constant'
DATA_OBJECT_NAME = 'data_object_name'
UNSPECIFIED_VARIABLE_NAME = 'unspecified'