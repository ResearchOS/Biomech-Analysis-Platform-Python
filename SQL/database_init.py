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
    def __init__(self):        
        self.conn = sqlite3.connect('./SQL/database.db')
        self.create_database()
        self.conn.commit()

    def create_triggers(self):
        cursor = self.conn.cursor()
        for table in object_table_names:
            try:
                cursor.execute("""CREATE TRIGGER update_timestamp_""" + table + """
                            AFTER UPDATE OF name, description ON """ + table + """
                            BEGIN 
                                UPDATE """ + table + """ SET updated_at = CURRENT_TIMESTAMP WHERE uuid = NEW.uuid;
                            END;""")
            except sqlite3.OperationalError:
                pass

    def create_database(self):
        """Create the data database and all of its tables."""
        cursor = self.conn.cursor()
        # Datasets table
        cursor.execute("""CREATE TABLE IF NOT EXISTS datasets (
                        uuid TEXT PRIMARY KEY, 
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP, 
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP
                        )""")

        # Subjects table
        cursor.execute("""CREATE TABLE IF NOT EXISTS subjects (
                        uuid TEXT PRIMARY KEY, 
                        dataset_uuid TEXT, 
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP, 
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (dataset_uuid) REFERENCES datasets(uuid) ON DELETE CASCADE
                        )""")

        # Visits table
        cursor.execute("""CREATE TABLE IF NOT EXISTS visits (
                        uuid TEXT PRIMARY KEY,   
                        subject_uuid TEXT NOT NULL,                      
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP, 
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (subject_uuid) REFERENCES subjects(uuid) ON DELETE CASCADE
                        )""")

        # Trials table
        cursor.execute("""CREATE TABLE IF NOT EXISTS trials (
                        uuid TEXT PRIMARY KEY,   
                        visit_uuid TEXT NOT NULL,                      
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP, 
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (visit_uuid) REFERENCES visits(uuid) ON DELETE CASCADE
                        )""")

        # Phases table. Phases are a many to many relationship with trials.
        cursor.execute("""CREATE TABLE IF NOT EXISTS phases (
                        uuid TEXT PRIMARY KEY,
                        phase_name TEXT NOT NULL DEFAULT 'Untitled',
                        start_uuid_var TEXT NOT NULL,
                        end_uuid_var TEXT NOT NULL,
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP,
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (start_uuid_var) REFERENCES variables(uuid) ON DELETE CASCADE,
                        FOREIGN KEY (end_uuid_var) REFERENCES variables(uuid) ON DELETE CASCADE                       
                        )""")
        
        # Variables table. Variables are a many to many relationship with phases.
        cursor.execute("""CREATE TABLE IF NOT EXISTS variables (
                        uuid TEXT PRIMARY KEY, 
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP, 
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP
                        )""")
        
        # Subvariables table. Subvariables are a many to many relationship with variables.
        cursor.execute("""CREATE TABLE IF NOT EXISTS subvariables (
                        uuid TEXT PRIMARY KEY,                    
                        subvar_index TEXT NOT NULL DEFAULT 'Untitled',
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP, 
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP                     
                        )""")
        
        # Dataset data
        cursor.execute("""CREATE TABLE IF NOT EXISTS dataset_data (
                        dataset_uuid TEXT NOT NULL,
                        var_uuid TEXT NOT NULL,
                        subvar_uuid TEXT,
                        value TEXT,
                        FOREIGN KEY (dataset_uuid) REFERENCES datasets(uuid) ON DELETE CASCADE,                        
                        FOREIGN KEY (var_uuid) REFERENCES variables(uuid) ON DELETE CASCADE,
                        FOREIGN KEY (subvar_uuid) REFERENCES subvariables(uuid) ON DELETE CASCADE
                        )""")
        
        # Subject data
        cursor.execute("""CREATE TABLE IF NOT EXISTS subject_data (
                        subject_uuid TEXT NOT NULL,
                        var_uuid TEXT NOT NULL,
                        subvar_uuid TEXT,
                        value TEXT,
                        FOREIGN KEY (subject_uuid) REFERENCES subjects(uuid) ON DELETE CASCADE,                       
                        FOREIGN KEY (var_uuid) REFERENCES variables(uuid) ON DELETE CASCADE,
                        FOREIGN KEY (subvar_uuid) REFERENCES subvariables(uuid) ON DELETE CASCADE
                        )""")
        
        # Visit data
        cursor.execute("""CREATE TABLE IF NOT EXISTS visit_data (
                        visit_uuid TEXT NOT NULL,
                        var_uuid TEXT NOT NULL,
                        subvar_uuid TEXT,
                        value TEXT,
                        FOREIGN KEY (visit_uuid) REFERENCES visits(uuid) ON DELETE CASCADE,                        
                        FOREIGN KEY (var_uuid) REFERENCES variables(uuid) ON DELETE CASCADE ,
                        FOREIGN KEY (subvar_uuid) REFERENCES subvariables(uuid) ON DELETE CASCADE                  
                        )""")

        # Phase-level data.
        # TODO: Wrap every call to INSERT/UPDATE records in this table with a "Check constraint" to ensure that exactly one of "file_path" OR "scalar_data" is null.
        # TODO: Also check constraint that subvar_uuid should only be not NULL if scalar_data is not NULL. Otherwise, all subvars just go into the file.
        # If phase_uuid is NULL, then the data is for the trial as a whole.
        # If subvar_uuid is NULL, then the data is for the variable as a whole.        
        cursor.execute("""CREATE TABLE IF NOT EXISTS phase_data (                        
                        trial_uuid TEXT NOT NULL,
                        phase_uuid TEXT,
                        var_uuid TEXT NOT NULL,
                        subvar_uuid TEXT,
                        file_path TEXT,
                        scalar_data TEXT,
                        FOREIGN KEY (trial_uuid) REFERENCES trials(uuid) ON DELETE CASCADE,                 
                        FOREIGN KEY (phase_uuid) REFERENCES phases(uuid) ON DELETE CASCADE,
                        FOREIGN KEY (var_uuid) REFERENCES variables(uuid) ON DELETE CASCADE,
                        FOREIGN KEY (subvar_uuid) REFERENCES subvariables(uuid) ON DELETE CASCADE,
                        PRIMARY KEY (trial_uuid, phase_uuid, var_uuid, subvar_uuid)
                        )""")

        self.create_triggers()   
        
if __name__ == '__main__':
    db = DBInitializer()
    