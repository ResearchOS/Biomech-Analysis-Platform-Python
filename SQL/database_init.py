"""Initialize a database to handle all of the data for the application."""

import sqlite3

class DBInitializer():
    def __init__(self, db_file: str = './SQL/database.db'):        
        self._conn = sqlite3.connect(db_file)
        self.create_database()
        self._conn.commit()

    def create_database(self):
        """Create the database and all of its tables."""
        cursor = self._conn.cursor()

        # Objects table. Lists all research objects in the database.
        cursor.execute("""CREATE TABLE IF NOT EXISTS objects (
                        object_id TEXT PRIMARY KEY
                        )""")

        # Current user table.
        cursor.execute("""CREATE TABLE IF NOT EXISTS Current_User (
                        current_user_object_id TEXT,
                        FOREIGN KEY (current_user_object_id) REFERENCES objects(object_id) ON DELETE CASCADE
                        )""")

        # Settings table. Contains all settings for the application.
        cursor.execute("""CREATE TABLE IF NOT EXISTS settings (
                        setting_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_object_id TEXT NOT NULL,
                        setting_name TEXT NOT NULL,
                        setting_value TEXT NOT NULL,
                        FOREIGN KEY (user_object_id) REFERENCES objects(object_id) ON DELETE CASCADE                        
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
                        FOREIGN KEY (dataset_id) REFERENCES objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")

        
if __name__ == '__main__':
    db = DBInitializer()
    