"""Initialize a database to handle all of the data for the application."""

from user import User
import sqlite3

class DBInitializer():
    def __init__(self, db_file: str = './SQL/database.db'):        
        self._conn = sqlite3.connect(db_file)
        self.create_database()
        self._conn.commit()
        self.init_values()
        self._conn.close()

    def init_values(self):
        """Initialize the values in the database."""
        # Current User
        default_user_object_id = "US000000_000"
        user = User.load(default_user_object_id)
    

    def create_database(self):
        """Create the database and all of its tables."""
        cursor = self._conn.cursor()

        # Objects table. Lists all research objects in the database.
        cursor.execute("""CREATE TABLE IF NOT EXISTS research_objects (
                        object_id TEXT PRIMARY KEY
                        )""")

        # Current user table.
        cursor.execute("""CREATE TABLE IF NOT EXISTS current_user (
                        current_user_object_id TEXT,
                        FOREIGN KEY (current_user_object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE
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
                        timestamp_opened TEXT NOT NULL,
                        timestamp_closed TEXT,
                        redo_of TEXT,
                        FOREIGN KEY (user_object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE
                        FOREIGN KEY (redo_of) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")

        # Attributes table. Lists all attributes that have been added to objects.
        cursor.execute("""CREATE TABLE IF NOT EXISTS attributes (
                        attr_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        attr_name TEXT NOT NULL
                        )""")

        # Research objects attributes table. Lists all attributes that have been associated with research objects.
        cursor.execute("""CREATE TABLE IF NOT EXISTS research_object_attributes (
                        action_id TEXT NOT NULL,
                        object_id TEXT NOT NULL,
                        attr_id INTEGER NOT NULL,
                        attr_value TEXT,
                        child_of TEXT,
                        FOREIGN KEY (attr_id) REFERENCES attributes(attr_id) ON DELETE CASCADE,
                        FOREIGN KEY (object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (child_of) REFERENCES research_objects(object_id) ON DELETE CASCADE,
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
                        FOREIGN KEY (schema_id) REFERENCES data_address_schemas(schema_id) ON DELETE CASCADE                        
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
    