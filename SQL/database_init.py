"""Initialize a database to handle all of the data for the application."""

import sqlite3

object_table_names = [
    'datasets',
    'subjects',
    'visits',
    'trials',
    'phases',
    'variables',
    'subvariables'    
]

data_table_names = [
    'dataset_data',
    'subject_data',
    'visit_data',
    'phase_data'
]

class DBInitializer():
    def __init__(self, db_file: str = './SQL/database.db'):        
        self._conn = sqlite3.connect(db_file)
        self.create_database()
        self._conn.commit()

    def create_database2(self):
        """Create the database and all of its tables."""
        cursor = self._conn.cursor()

        # Current user table.
        cursor.execute("""CREATE TABLE IF NOT EXISTS Current_User (
                        current_user_object_id TEXT FOREIGN KEY REFERENCES objects(object_id) ON DELETE CASCADE
                        )""")

        # Settings table. Contains all settings for the application.
        cursor.execute("""CREATE TABLE IF NOT EXISTS settings (
                        setting_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_object_id TEXT NOT NULL,
                        setting_name TEXT NOT NULL,
                        setting_value TEXT NOT NULL
                        )""")
        
        # Objects table. Lists all research objects in the database.
        cursor.execute("""CREATE TABLE IF NOT EXISTS objects (
                        object_id TEXT PRIMARY KEY,
                        )""")

        # Actions table. Lists all actions that have been performed, and their timestamps.
        cursor.execute("""CREATE TABLE IF NOT EXISTS actions (
                        action_id TEXT PRIMARY KEY,
                        timestamp_opened TEXT NOT NULL,
                        timestamp_closed TEXT
                        )""")

        # Attributes table. Lists all attributes that have been added to objects.
        cursor.execute("""CREATE TABLE IF NOT EXISTS attributes (
                        attr_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        attr_name TEXT NOT NULL
                        )""")

        # Research objects transactions table. Lists all transactions that have been performed on research objects.
        cursor.execute("""CREATE TABLE IF NOT EXISTS object_transactions (
                        action_id TEXT NOT NULL,
                        object_id TEXT NOT NULL,
                        attr_id INTEGER NOT NULL,
                        attr_value TEXT,
                        child_of TEXT,
                        FOREIGN KEY (attr_id) REFERENCES attributes(attr_id) ON DELETE CASCADE,
                        FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (child_of) REFERENCES objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")
        
        # Data objects data values. Lists all data values for all data objects.
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_values (
                        action_id TEXT NOT NULL,
                        address_id TEXT NOT NULL,
                        schema_id TEXT NOT NULL,
                        VR_id TEXT NOT NULL,
                        PR_id TEXT NOT NULL,
                        scalar_value TEXT,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE,
                        FOREIGN KEY (address_id) REFERENCES data_addresses(address_id) ON DELETE CASCADE,
                        FOREIGN KEY (schema_id) REFERENCES data_address_schemas(schema_id) ON DELETE CASCADE
                        )""")
        
        # Data addresses. Lists all data addresses for all data.
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_addresses (
                        address_id TEXT PRIMARY KEY,
                        schema_id TEXT,
                        level0_value TEXT,
                        level1_value TEXT,
                        level2_value TEXT,
                        level3_value TEXT,
                        level4_value TEXT,
                        level5_value TEXT,
                        level6_value TEXT,
                        level7_value TEXT,
                        level8_value TEXT,
                        level9_value TEXT,                        
                        FOREIGN KEY (schema_id) REFERENCES data_address_schemas(schema_id) ON DELETE CASCADE                        
                        )""")
        
        # Data address schemas. Lists all data address schemas for all data.
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_address_schemas (
                        action_id TEXT NOT NULL,
                        schema_id TEXT NOT NULL,
                        dataset_id TEXT NOT NULL,
                        level_name TEXT NOT NULL,
                        level_index TEXT NOT NULL,
                        FOREIGN KEY (dataset_id) REFERENCES objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE,
                        )""")


    def create_database(self):
        """Create the data database and all of its tables."""
        cursor = self._conn.cursor()
        # Objects table
        cursor.execute("""CREATE TABLE IF NOT EXISTS objects (
                       object_id TEXT PRIMARY KEY,
                       date_created TEXT DEFAULT CURRENT_TIMESTAMP
                      )""")

        # Transactions table
        cursor.execute("""CREATE TABLE IF NOT EXISTS transactions (
                       transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                       object_id TEXT NOT NULL DEFAULT 'ZZZZZZ_ZZZ',
                       attr_id INTEGER NOT NULL DEFAULT 0,
                       attr_value TEXT,
                       FOREIGN KEY (attr_id) REFERENCES attributes(attr_id) ON DELETE CASCADE
                       FOREIGN KEY (object_id) REFERENCES objects(object_id) ON DELETE CASCADE
                       )""")

        # Attributes table
        cursor.execute("""CREATE TABLE IF NOT EXISTS attributes (
                       attr_id INTEGER PRIMARY KEY AUTOINCREMENT,
                       attr_name TEXT NOT NULL,    
                        )""")
        
        # Times table
        cursor.execute("""CREATE TABLE IF NOT EXISTS times (
                       id INTEGER PRIMARY KEY AUTOINCREMENT,
                       transaction_id INTEGER NOT NULL,
                       start_time TEXT NOT NULL,
                       end_time TEXT,  
                       FOREIGN KEY (transaction_id) REFERENCES transactions(transaction_id) ON DELETE CASCADE
                        )""")

        # Data table??
        """Contains all data of all types, and a reference to which object it is derived from/is the child of."""

        # Runs table??
        """Lists all runs of each processing step."""


        # # Datasets table
        # cursor.execute("""CREATE TABLE IF NOT EXISTS datasets (
        #                 uuid TEXT PRIMARY KEY,
        #                 id TEXT NOT NULL DEFAULT 'ZZZZZZ_ZZZ',
        #                 name TEXT NOT NULL DEFAULT 'Untitled', 
        #                 description TEXT,
        #                 timestamp TEXT DEFAULT CURRENT_TIMESTAMP
        #                 )""")

        # # Subjects table
        # cursor.execute("""CREATE TABLE IF NOT EXISTS subjects (
        #                 uuid TEXT PRIMARY KEY, 
        #                 id TEXT NOT NULL DEFAULT 'ZZZZZZ_ZZZ',
        #                 dataset_uuid TEXT, 
        #                 name TEXT NOT NULL DEFAULT 'Untitled', 
        #                 description TEXT, 
        #                 timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
        #                 FOREIGN KEY (dataset_uuid) REFERENCES datasets(uuid) ON DELETE CASCADE
        #                 )""")

        # # Visits table
        # cursor.execute("""CREATE TABLE IF NOT EXISTS visits (
        #                 uuid TEXT PRIMARY KEY,  
        #                 id TEXT NOT NULL DEFAULT 'ZZZZZZ_ZZZ', 
        #                 subject_uuid TEXT NOT NULL,                      
        #                 name TEXT NOT NULL DEFAULT 'Untitled', 
        #                 description TEXT, 
        #                 timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
        #                 FOREIGN KEY (subject_uuid) REFERENCES subjects(uuid) ON DELETE CASCADE
        #                 )""")

        # # Trials table
        # cursor.execute("""CREATE TABLE IF NOT EXISTS trials (
        #                 uuid TEXT PRIMARY KEY,   
        #                 id TEXT NOT NULL DEFAULT 'ZZZZZZ_ZZZ',
        #                 visit_uuid TEXT NOT NULL,                      
        #                 name TEXT NOT NULL DEFAULT 'Untitled', 
        #                 description TEXT, 
        #                 timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
        #                 FOREIGN KEY (visit_uuid) REFERENCES visits(uuid) ON DELETE CASCADE
        #                 )""")

        # # Phases table. Phases are a many to many relationship with trials.
        # cursor.execute("""CREATE TABLE IF NOT EXISTS phases (
        #                 uuid TEXT PRIMARY KEY,
        #                 id TEXT NOT NULL DEFAULT 'ZZZZZZ_ZZZ',
        #                 name TEXT NOT NULL DEFAULT 'Untitled',
        #                 description TEXT,
        #                 start_uuid_var TEXT,
        #                 end_uuid_var TEXT,
        #                 timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
        #                 FOREIGN KEY (start_uuid_var) REFERENCES variables(uuid) ON DELETE CASCADE,
        #                 FOREIGN KEY (end_uuid_var) REFERENCES variables(uuid) ON DELETE CASCADE                       
        #                 )""")
        
        # # Variables table. Variables are a many to many relationship with phases.
        # cursor.execute("""CREATE TABLE IF NOT EXISTS variables (
        #                 uuid TEXT PRIMARY KEY, 
        #                 id TEXT NOT NULL DEFAULT 'ZZZZZZ_ZZZ',
        #                 name TEXT NOT NULL DEFAULT 'Untitled', 
        #                 description TEXT, 
        #                 timestamp TEXT DEFAULT CURRENT_TIMESTAMP
        #                 )""")
        
        # # Subvariables table. Subvariables are a many to many relationship with variables.
        # cursor.execute("""CREATE TABLE IF NOT EXISTS subvariables (
        #                 uuid TEXT PRIMARY KEY,    
        #                 id TEXT NOT NULL DEFAULT 'ZZZZZZ_ZZZ',
        #                 name TEXT NOT NULL DEFAULT 'Untitled',  
        #                 description TEXT,              
        #                 subvar_index TEXT NOT NULL DEFAULT 'Untitled',
        #                 timestamp TEXT DEFAULT CURRENT_TIMESTAMP                        
        #                 )""")
        
        # # Dataset data
        # cursor.execute("""CREATE TABLE IF NOT EXISTS dataset_data (
        #                 dataset_uuid TEXT NOT NULL,
        #                 var_uuid TEXT NOT NULL,
        #                 subvar_uuid TEXT,
        #                 value TEXT,
        #                 FOREIGN KEY (dataset_uuid) REFERENCES datasets(uuid) ON DELETE CASCADE,                        
        #                 FOREIGN KEY (var_uuid) REFERENCES variables(uuid) ON DELETE CASCADE,
        #                 FOREIGN KEY (subvar_uuid) REFERENCES subvariables(uuid) ON DELETE CASCADE
        #                 )""")
        
        # # Subject data
        # cursor.execute("""CREATE TABLE IF NOT EXISTS subject_data (
        #                 subject_uuid TEXT NOT NULL,
        #                 var_uuid TEXT NOT NULL,
        #                 subvar_uuid TEXT,
        #                 value TEXT,
        #                 FOREIGN KEY (subject_uuid) REFERENCES subjects(uuid) ON DELETE CASCADE,                       
        #                 FOREIGN KEY (var_uuid) REFERENCES variables(uuid) ON DELETE CASCADE,
        #                 FOREIGN KEY (subvar_uuid) REFERENCES subvariables(uuid) ON DELETE CASCADE
        #                 )""")
        
        # # Visit data
        # cursor.execute("""CREATE TABLE IF NOT EXISTS visit_data (
        #                 visit_uuid TEXT NOT NULL,
        #                 var_uuid TEXT NOT NULL,
        #                 subvar_uuid TEXT,
        #                 value TEXT,
        #                 FOREIGN KEY (visit_uuid) REFERENCES visits(uuid) ON DELETE CASCADE,                        
        #                 FOREIGN KEY (var_uuid) REFERENCES variables(uuid) ON DELETE CASCADE ,
        #                 FOREIGN KEY (subvar_uuid) REFERENCES subvariables(uuid) ON DELETE CASCADE                  
        #                 )""")

        # # Phase-level data.
        # # TODO: Wrap every call to INSERT/UPDATE records in this table with a "Check constraint" to ensure that exactly one of "file_path" OR "scalar_data" is null.
        # # TODO: Also check constraint that subvar_uuid should only be not NULL if scalar_data is not NULL. Otherwise, all subvars just go into the file.
        # # If phase_uuid is NULL, then the data is for the trial as a whole.
        # # If subvar_uuid is NULL, then the data is for the variable as a whole.        
        # cursor.execute("""CREATE TABLE IF NOT EXISTS phase_data (                        
        #                 trial_uuid TEXT NOT NULL,
        #                 phase_uuid TEXT,
        #                 var_uuid TEXT NOT NULL,
        #                 subvar_uuid TEXT,
        #                 file_path TEXT,
        #                 scalar_data TEXT,
        #                 FOREIGN KEY (trial_uuid) REFERENCES trials(uuid) ON DELETE CASCADE,                 
        #                 FOREIGN KEY (phase_uuid) REFERENCES phases(uuid) ON DELETE CASCADE,
        #                 FOREIGN KEY (var_uuid) REFERENCES variables(uuid) ON DELETE CASCADE,
        #                 FOREIGN KEY (subvar_uuid) REFERENCES subvariables(uuid) ON DELETE CASCADE,
        #                 PRIMARY KEY (trial_uuid, phase_uuid, var_uuid, subvar_uuid)
        #                 )""")

        # self.create_triggers()   
        
if __name__ == '__main__':
    db = DBInitializer()
    