"""Initialize a database to handle all of the data for the application."""

import sqlite3

class DBManager():
    def __init__(self):        
        self.create_database()

    def create_database(self):
        """Create the data database and all of its tables."""
        self.conn = sqlite3.connect('database.db')
        cursor = self.conn.cursor()
        # Datasets table
        cursor.execute("""CREATE TABLE IF NOT EXISTS datasets (
                        id INTEGER PRIMARY KEY, 
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP, 
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                        )""")                

        # Subjects table
        cursor.execute("""CREATE TABLE IF NOT EXISTS subjects (
                        id INTEGER PRIMARY KEY, 
                        dataset_id INTEGER NOT NULL, 
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP, 
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (dataset_id) REFERENCES datasets(id)
                        )""")

        # Visits table
        cursor.execute("""CREATE TABLE IF NOT EXISTS visits (
                        id INTEGER PRIMARY KEY,   
                        subject_id INTEGER NOT NULL,                      
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP, 
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (subject_id) REFERENCES subjects(id)
                        )""")

        # Trials table
        cursor.execute("""CREATE TABLE IF NOT EXISTS trials (
                        id INTEGER PRIMARY KEY,   
                        visit_id INTEGER NOT NULL,                      
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP, 
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (visit_id) REFERENCES visits(id)
                        )""")

        # Phases table. Needs another two tables to say what the human-readable name of the phase is.
        # Table 1: phase_id (unique to the trial), phase_name_id
        # Table 2: phase_name_id, phase_name
        cursor.execute("""CREATE TABLE IF NOT EXISTS phases (
                        id INTEGER PRIMARY KEY,
                        trial_id INTEGER NOT NULL,                     
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP, 
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (trial_id) REFERENCES trials(id)
                       )""")
        
        # Phase name ids table. Connects phases to trials.
        cursor.execute("""CREATE TABLE IF NOT EXISTS phase_name_ids (
                        phase_trial_id INTEGER PRIMARY KEY,
                        trial_id INTEGER NOT NULL,
                        phase_id TEXT NOT NULL DEFAULT 'Untitled',
                        FOREIGN KEY (phase_id) REFERENCES phase_names(id)
                        )""")
        
        # Phase name values table. Connects phase ID's to human-readable names (independent of trial)
        # E.g.: phase_id = 1, phase_name = 'turn phase heel strike'
        # TODO: Maybe also connect this to a phase-generating group or function?
        cursor.execute("""CREATE TABLE IF NOT EXISTS phase_names (
                        phase_id INTEGER,
                        phase_name TEXT NOT NULL DEFAULT 'Untitled',
                        FOREIGN KEY (phase_name_id) REFERENCES phase_name_ids(id)
                        )""")
        
        # List of all variables
        cursor.execute("""CREATE TABLE IF NOT EXISTS variables (
                        id INTEGER PRIMARY KEY, 
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP, 
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
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

        # Phase-level data. This is for all non-final processed data, especially non-scalar data.
        # NOTE: The phase_id is unique to each trial, so no need to reference a trial_id here.
        cursor.execute("""CREATE TABLE IF NOT EXISTS trial_phase_data (
                        phase_id INTEGER NOT NULL,
                        var_id INTEGER NOT NULL,
                        file_path TEXT NOT NULL DEFAULT 'NULL',
                        FOREIGN KEY (trial_id) REFERENCES trial_id(id),                 
                        FOREIGN KEY (var_id) REFERENCES variables(id),
                        PRIMARY KEY (trial_id, phase_id, var_id)
                        )""")
        
        # Trial & phase data. This is for all final processed, scalar data.
        cursor.execute("""CREATE TABLE IF NOT EXISTS trial_phase_data_processed (
                        phase_id INTEGER NOT NULL,
                        var_id INTEGER NOT NULL,
                        scalar_data TEXT,
                        FOREIGN KEY (trial_id) REFERENCES trial_id(id),                 
                        FOREIGN KEY (var_id) REFERENCES variables(id),
                        PRIMARY KEY (trial_id, phase_id, var_id)
                        )""")        
        
if __name__ == '__main__':
    db = DBManager()