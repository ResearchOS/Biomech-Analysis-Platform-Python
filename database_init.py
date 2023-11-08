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
                       )"""
        )

        # Phases table
        cursor.execute("""CREATE TABLE IF NOT EXISTS phases (
                        id INTEGER PRIMARY KEY,    
                        trial_id INTEGER NOT NULL,                     
                        name TEXT NOT NULL DEFAULT 'Untitled', 
                        description TEXT, 
                        created_at TEXT DEFAULT CURRENT_TIMESTAMP, 
                        updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (trial_id) REFERENCES trials(id)
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

        # Trial data. For all non-scalar data, the file_path field will be used to store the path to the file.
        cursor.execute("""CREATE TABLE IF NOT EXISTS trial_phase_data (
                        trial_phase_id INTEGER NOT NULL,
                        var_id INTEGER NOT NULL,
                        file_path TEXT NOT NULL DEFAULT 'NULL',
                        scalar_data TEXT NOT NULL DEFAULT 'NULL',
                        FOREIGN KEY (trial_phase_id) REFERENCES trial_phase_id(id),                 
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
        
        # Trial_phase_id
        cursor.execute("""CREATE TABLE IF NOT EXISTS trial_phase_id (
                        id INTEGER PRIMARY KEY,
                        trial_id INTEGER NOT NULL,
                        phase_id INTEGER NOT NULL,
                        FOREIGN KEY (trial_id) REFERENCES trials(id),    
                        FOREIGN KEY (phase_id) REFERENCES phases(id)                  
                       )""")
        
if __name__ == '__main__':
    db = DBManager()