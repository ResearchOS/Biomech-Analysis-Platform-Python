"""Initialize a database to handle all of the data for the application."""

import sqlite3, os, json

from ResearchOS.current_user import CurrentUser
from ResearchOS.db_connection_factory import DBConnectionFactory

sql_settings_path = os.path.dirname(__file__) + "/config/sql.json"

class DBInitializer():
    """Initializes the database when the Python package is run for the first time using ros-quickstart."""

    def __init__(self, db_file: str) -> None:
        """Initialize the database."""
        with open(sql_settings_path, "r") as file:
            data = json.load(file) 
        intended_tables = data["intended_tables"]

        if os.path.exists(db_file):
            os.remove(db_file)

        self.db_file = db_file
        conn = sqlite3.connect(db_file)
        self.conn = DBConnectionFactory.create_db_connection(db_file).conn
        # self.conn = sqlite3.connect(db_file)
        self.create_tables()
        self.check_tables_exist(intended_tables)
        self.init_current_user_id()

    def init_current_user_id(self, user_id: str = "US000000_000"):
        """Initialize the current user ID in the settings table."""
        CurrentUser(self.conn).set_current_user_id(user_id)

    def check_tables_exist(self, intended_tables: list):
        """Check that all of the tables were created."""        
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        tables = [table[0] for table in tables]
        missing_tables = [table for table in intended_tables if table not in tables]
        if missing_tables:
            print(missing_tables)
            raise Exception("At least one table missing!")

    def create_tables(self):
        """Create the database and all of its tables."""
        cursor = self.conn.cursor()

        # Action-tables one-to-many relation table. Lists all actions and which tables they had an effect on.
        cursor.execute("""CREATE TABLE IF NOT EXISTS action_tables (
                        action_id TEXT NOT NULL,
                        table_name TEXT NOT NULL,
                        PRIMARY KEY (action_id, table_name),
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")

        # Objects table. Lists all research objects in the database, and which action created them.
        cursor.execute("""CREATE TABLE IF NOT EXISTS research_objects (
                        object_id TEXT PRIMARY KEY,
                        action_id TEXT NOT NULL,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")

        # Actions table. Lists all actions that have been performed, and their timestamps.
        cursor.execute("""CREATE TABLE IF NOT EXISTS actions (
                        action_id TEXT PRIMARY KEY,
                        user TEXT NOT NULL,
                        name TEXT NOT NULL,
                        datetime TEXT NOT NULL,                        
                        redo_of TEXT,
                        FOREIGN KEY (user) REFERENCES research_objects(object_id) ON DELETE CASCADE
                        FOREIGN KEY (redo_of) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")

        # Attributes table. Lists all attributes that have been added to objects.
        # NOTE: attr_type is NOT JSON, it is just a str representation of a class name.
        cursor.execute("""CREATE TABLE IF NOT EXISTS attributes_list (
                        attr_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        attr_name TEXT NOT NULL
                        )""")

        # Simple attributes table. Lists all "simple" (i.e. json-serializable) attributes that have been associated with research objects.
        cursor.execute("""CREATE TABLE IF NOT EXISTS simple_attributes (
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
        # address_id is just the lowest level data object ID (the lowest level of the address).
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_values (
                        action_id TEXT NOT NULL,
                        address_id TEXT NOT NULL,
                        schema_id TEXT NOT NULL,
                        VR_id TEXT NOT NULL,
                        scalar_value TEXT,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE,
                        FOREIGN KEY (address_id) REFERENCES data_addresses(address_id) ON DELETE CASCADE,
                        FOREIGN KEY (schema_id) REFERENCES data_address_schemas(schema_id) ON DELETE CASCADE,
                        FOREIGN KEY (VR_id) REFERENCES research_objects(object_id) ON DELETE CASCADE                    
                        )""")
        
        # Data addresses. Lists all data addresses for all data.
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_addresses (
                        action_id TEXT NOT NULL,
                        address_id TEXT NOT NULL,
                        schema_id TEXT NOT NULL,
                        level0_id TEXT,
                        level1_id TEXT,
                        level2_id TEXT,
                        level3_id TEXT,
                        level4_id TEXT,
                        level5_id TEXT,
                        level6_id TEXT,
                        level7_id TEXT,
                        level8_id TEXT,
                        level9_id TEXT,
                        FOREIGN KEY (schema_id) REFERENCES data_address_schemas(schema_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE,
                        FOREIGN KEY (address_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (level0_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (level1_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (level2_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (level3_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (level4_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (level5_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (level6_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (level7_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (level8_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (level9_id) REFERENCES research_objects(object_id) ON DELETE CASCADE
                        )""")
        
        # Data address schemas. Lists all data address schemas for all data.
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_address_schemas (
                        action_id TEXT NOT NULL,
                        schema_id TEXT NOT NULL,
                        dataset_id TEXT NOT NULL,
                        levels_edge_list TEXT NOT NULL,
                        FOREIGN KEY (dataset_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")


        
if __name__ == '__main__':    
    db = DBInitializer("dev_database.db")