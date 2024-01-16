"""Initialize a database to handle all of the data for the application."""

import sqlite3, os, datetime
import sys
sys.path.append("/Users/mitchelltillman/Desktop/Not_Work/Code/Python_Projects/Biomech-Analysis-Platform-Python/src")
import ResearchOS as ros

# from ResearchOS.config import Config
config = ros.Config()

DEFAULT_USER_ID = "US000000_000"

intended_tables = [
    "research_objects", "actions", "attributes", "research_object_attributes", "data_values", "data_addresses", "data_address_schemas", "current_user", "settings"
]

class DBInitializer():
    def __init__(self):        
        db_file = config.db_file
        if os.path.exists(db_file):
            os.remove(db_file)        
        self._conn = sqlite3.connect(db_file)   
        full_file = os.path.abspath(db_file)         
        folder = os.path.dirname(full_file)
        os.chmod(full_file, 0o755)
        os.chmod(folder, 0o755)
        self.create_database()
        self._conn.commit()
        self.check_tables_exist()
        self.init_current_user_id()        
        self._conn.cursor().close()
        self._conn.close()

    def check_tables_exist(self):
        """Check that all of the tables were created."""
        cursor = self._conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        for table in intended_tables:
            if table not in table_names:
                raise Exception(f"Table {table} was not created.")
            
    def init_current_user_id(self, user_id: str = DEFAULT_USER_ID):
        """Initialize the current user ID."""
        from ResearchOS.action import Action
        cursor = self._conn.cursor()
        sqlquery = f"INSERT INTO research_objects (object_id) VALUES ('{user_id}')"
        cursor.execute(sqlquery)
        self._conn.commit()
        action_id = Action._create_uuid()
        sqlquery = f"INSERT INTO current_user (action_id, current_user_object_id) VALUES ('{action_id}', '{user_id}')"        
        cursor.execute(sqlquery)
        sqlquery = f"INSERT INTO actions (action_id, user_object_id, name, timestamp) VALUES ('{action_id}', '{user_id}', 'Initialize current user', '{datetime.datetime.now(datetime.UTC)}')"
        cursor.execute(sqlquery)
        self._conn.commit()             

    def create_database(self):
        """Create the database and all of its tables."""
        cursor = self._conn.cursor()

        # Objects table. Lists all research objects in the database.
        cursor.execute("""CREATE TABLE IF NOT EXISTS research_objects (
                        object_id TEXT PRIMARY KEY
                        )""")

        # Current user table.
        cursor.execute("""CREATE TABLE IF NOT EXISTS current_user (
                        action_id TEXT PRIMARY KEY,
                        current_user_object_id TEXT,
                        FOREIGN KEY (current_user_object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")

        # Settings table. Contains all settings for the application.
        cursor.execute("""CREATE TABLE IF NOT EXISTS settings (
                        setting_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_object_id TEXT NOT NULL,
                        setting_name TEXT NOT NULL,
                        setting_value TEXT NOT NULL,
                        FOREIGN KEY (user_object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE                        
                        )""")        

        # Actions table. Lists all actions that have been performed, and their timestamps.
        cursor.execute("""CREATE TABLE IF NOT EXISTS actions (
                        action_id TEXT PRIMARY KEY,
                        user_object_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        timestamp TEXT NOT NULL,                        
                        redo_of TEXT,
                        FOREIGN KEY (user_object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE
                        FOREIGN KEY (redo_of) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")

        # Attributes table. Lists all attributes that have been added to objects.
        # NOTE: attr_type is NOT JSON, it is just a str representation of a class name.
        cursor.execute("""CREATE TABLE IF NOT EXISTS attributes (
                        attr_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        attr_name TEXT NOT NULL,
                        attr_type TEXT
                        )""")

        # Research objects attributes table. Lists all attributes that have been associated with research objects.
        cursor.execute("""CREATE TABLE IF NOT EXISTS research_object_attributes (
                        action_row_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        action_id TEXT NOT NULL,
                        object_id TEXT NOT NULL,
                        attr_id INTEGER NOT NULL,
                        attr_value TEXT,
                        target_object_id TEXT,
                        FOREIGN KEY (attr_id) REFERENCES attributes(attr_id) ON DELETE CASCADE,
                        FOREIGN KEY (object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (target_object_id) REFERENCES research_objects(target_object_id) ON DELETE CASCADE,
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
                        FOREIGN KEY (schema_id) REFERENCES data_address_schemas(schema_id) ON DELETE CASCADE,
                        FOREIGN KEY (VR_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (PR_id) REFERENCES research_objects(object_id) ON DELETE CASCADE                        
                        )""")
        
        # Data addresses. Lists all data addresses for all data.
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_addresses (
                        action_id TEXT NOT NULL,
                        address_id TEXT NOT NULL,
                        schema_id TEXT NOT NULL,
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
                        FOREIGN KEY (schema_id) REFERENCES data_address_schemas(schema_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")
        
        # Data address schemas. Lists all data address schemas for all data.
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_address_schemas (
                        action_id TEXT NOT NULL,
                        schema_id TEXT NOT NULL,
                        dataset_id TEXT NOT NULL,
                        level_name TEXT NOT NULL,
                        level_index TEXT NOT NULL,
                        FOREIGN KEY (dataset_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")

        
if __name__ == '__main__':    
    db = DBInitializer()