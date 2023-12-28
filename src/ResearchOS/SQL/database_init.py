"""Initialize a database to handle all of the data for the application."""

# import os, sys
# PROJECT_ROOT = os.path.abspath(os.path.join(
#                   os.path.dirname(__file__), 
#                   os.pardir)
# )
# sys.path.append(PROJECT_ROOT)

import sqlite3

class DBInitializer():
    def __init__(self, db_file: str):        
        self._conn = sqlite3.connect(db_file)
        self.create_database()
        self._conn.commit()
        self.init_values()
        self._conn.commit()
        self._conn.close()

    def init_values(self):
        """Initialize the values in the database."""
        from src.ResearchOS import User
        # Current User
        default_user_object_id = "US000000_000"
        sqlquery = f"INSERT INTO current_user (current_user_object_id) VALUES ('{default_user_object_id}')"
        cursor = self._conn.cursor()
        cursor.execute(sqlquery)
        self._conn.commit()
        user = User(id = default_user_object_id, name = "Default User")

        # Initialize attributes in the database, for some informal standardization.
        self.init_attr(name = "id")
        self.init_attr(name = "name")
        self.init_attr(name = "description")
        self.init_attr(name = "target_object_id")
        self.init_attr(name = "timestamp")
        self.init_attr(name = "redo_of")
        self.init_attr(name = "action_id")
        self.init_attr(name = "object_id")
        # self.init_attr(name = "attr_id")
        # self.init_attr(name = "attr_value")
        # self.init_attr(name = "child_of")
        self.init_attr(name = "address_id")
        self.init_attr(name = "schema_id")
        self.init_attr(name = "VR_id")
        self.init_attr(name = "PR_id")
        self.init_attr(name = "scalar_value")
        self.init_attr(name = "level0_value")
        self.init_attr(name = "level1_value")
        self.init_attr(name = "level2_value")
        self.init_attr(name = "level3_value")
        self.init_attr(name = "level4_value")
        self.init_attr(name = "level5_value")
        self.init_attr(name = "level6_value")
        self.init_attr(name = "level7_value")
        self.init_attr(name = "level8_value")
        self.init_attr(name = "level9_value")
        self.init_attr(name = "current_analysis_id")
        self.init_attr(name = "current_project_id")
        self.init_attr(name = "current_logsheet_id")
        # self.init_attr(name = "current_process_group_id")
        # self.init_attr(name = "current_process_id")
        # self.init_attr(name = "current_variable_id")
        self.init_attr(name = "current_subset_id")        
        self.init_attr(name = "current_user_object_id")
        self.init_attr(name = "current_action_id")


    def init_attr(self, name: str):
        """Initialize an attribute in the database."""
        cursor = self._conn.cursor()
        cursor.execute(f"INSERT INTO attributes (attr_name) VALUES ('{name}')")
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
                        timestamp TEXT NOT NULL,                        
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
                        target_object_id TEXT,
                        FOREIGN KEY (attr_id) REFERENCES attributes(attr_id) ON DELETE CASCADE,
                        FOREIGN KEY (object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (target_object_id) REFERENCES research_objects(target_object_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE,
                        PRIMARY KEY (action_id, object_id, attr_id)                        
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
    from src.ResearchOS.config import ProdConfig
    db = DBInitializer(ProdConfig.db_file)
    