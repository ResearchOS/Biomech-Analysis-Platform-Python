"""Initialize a database to handle all of the data for the application."""

import sqlite3

class DBInitializer():
    def __init__(self):        
        self.conn = sqlite3.connect('./SQL/database.db')
        self.create_database()
        self.conn.commit()

    def create_triggers(self):
        cursor = self.conn.cursor()
        # Datasets table
        try:
            cursor.execute("""CREATE TRIGGER update_timestamp_datasets
                        AFTER UPDATE OF name, description ON datasets
                        BEGIN 
                            UPDATE datasets SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
                        END;""")
        except sqlite3.OperationalError:
            pass
        
        # Subjects table
        try:
            cursor.execute("""CREATE TRIGGER update_timestamp_subjects
                        AFTER UPDATE OF codename, description ON subjects
                        BEGIN 
                            UPDATE subjects SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
                        END;""")
        except sqlite3.OperationalError:
            pass
        
        # Visits table
        try:
            cursor.execute("""CREATE TRIGGER update_timestamp_visits
                            AFTER UPDATE OF name, description ON visits
                            BEGIN 
                                UPDATE visits SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
                            END;""")
        except sqlite3.OperationalError:
            pass
        
        # Trials table
        try:
            cursor.execute("""CREATE TRIGGER update_timestamp_trials
                            AFTER UPDATE OF name, description ON trials
                            BEGIN 
                                UPDATE trials SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
                            END;""")
        except sqlite3.OperationalError:
            pass
        
        # Phases table
        try:
            cursor.execute("""CREATE TRIGGER update_timestamp_phases
                            AFTER UPDATE OF name, description ON phases
                            BEGIN 
                                UPDATE phases SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
                            END;""")
        except sqlite3.OperationalError:
            pass


    def create_database(self):
        """Create the data database and all of its tables."""
        cursor = self.conn.cursor()
        # Datasets table
        cursor.execute("""CREATE TABLE IF NOT EXISTS datasets (
                        id INTEGER PRIMARY KEY, 
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP, 
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP
                        )""")

        # Subjects table
        cursor.execute("""CREATE TABLE IF NOT EXISTS subjects (
                        id INTEGER PRIMARY KEY, 
                        dataset_id INTEGER NOT NULL, 
                        codename TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP, 
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (dataset_id) REFERENCES datasets(id)
                        )""")

        # Visits table
        cursor.execute("""CREATE TABLE IF NOT EXISTS visits (
                        id INTEGER PRIMARY KEY,   
                        subject_id INTEGER NOT NULL,                      
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP, 
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (subject_id) REFERENCES subjects(id)
                        )""")

        # Trials table
        cursor.execute("""CREATE TABLE IF NOT EXISTS trials (
                        id INTEGER PRIMARY KEY,   
                        visit_id INTEGER NOT NULL,                      
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP, 
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (visit_id) REFERENCES visits(id)
                        )""")

        # Phases table. The start and end indices are stored as variables, even if generated manually.
        cursor.execute("""CREATE TABLE IF NOT EXISTS phases (
                        id INTEGER PRIMARY KEY,
                        phase_name TEXT NOT NULL DEFAULT 'Untitled',
                        start_idx_var TEXT NOT NULL,
                        end_idx_var TEXT NOT NULL,
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP,
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (start_idx_var) REFERENCES variables(id),
                        FOREIGN KEY (end_idx_var) REFERENCES variables(id)                        
                        )""")
        
        # Trials -> phases table. If there is no phase for that trial (using the whole trial), then phase_id is NULL.
        # NOTE: If a phase is created manually, that data will still be stored to a variable.
        # So, I can treat all phases as though they were created by algorithms because all end results are still in variables.
        cursor.execute("""CREATE TABLE IF NOT EXISTS trial_phases (
                        trial_phase_id INTEGER PRIMARY KEY,
                        trial_id INTEGER NOT NULL,
                        phase_id INTEGER,
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP,
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (trial_id) REFERENCES trials(id),
                        FOREIGN KEY (phase_id) REFERENCES phases(id)
                        )""")
        
        # List of all variables
        cursor.execute("""CREATE TABLE IF NOT EXISTS variables (
                        id INTEGER PRIMARY KEY, 
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at INTEGER DEFAULT CURRENT_TIMESTAMP, 
                        updated_at INTEGER DEFAULT CURRENT_TIMESTAMP
                        )""")
        
        # List of all subvariables
        cursor.execute("""CREATE TABLE IF NOT EXISTS subvariables (
                        var_subvar_id INTEGER PRIMARY KEY,    
                        var_id INTEGER NOT NULL,                     
                        subvar_index TEXT NOT NULL DEFAULT 'Untitled',
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP, 
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (var_id) REFERENCES variables(id)                        
                        )""")
        
        # Subject data
        cursor.execute("""CREATE TABLE IF NOT EXISTS subject_data (
                        subject_id INTEGER NOT NULL,
                        var_id INTEGER NOT NULL,
                        value TEXT,
                        FOREIGN KEY (subject_id) REFERENCES subjects(id),                         
                        FOREIGN KEY (var_id) REFERENCES variables(id)                        
                        )""")
        
        # Visit data
        cursor.execute("""CREATE TABLE IF NOT EXISTS visit_data (
                        visit_id INTEGER NOT NULL,
                        var_id INTEGER NOT NULL,
                        value TEXT,
                        FOREIGN KEY (visit_id) REFERENCES visits(id),                        
                        FOREIGN KEY (var_id) REFERENCES variables(id)                      
                        )""")

        # Phase-level data. 
        # TODO: Wrap every call to INSERT/UPDATE records in this table with a "Check constraint" to ensure that exactly one of "file_path" OR "scalar_data" is null.
        cursor.execute("""CREATE TABLE IF NOT EXISTS phase_data (
                        trial_phase_id INTEGER NOT NULL,
                        var_subvar_id INTEGER NOT NULL,
                        file_path TEXT NOT NULL DEFAULT 'NULL',
                        scalar_data TEXT,
                        FOREIGN KEY (trial_phase_id) REFERENCES trial_phases(trial_phase_id),                 
                        FOREIGN KEY (var_subvar_id) REFERENCES subvariables(id),
                        PRIMARY KEY (trial_phase_id, var_subvar_id)
                        )""")

        self.create_triggers()   
        
if __name__ == '__main__':
    db = DBInitializer()
    