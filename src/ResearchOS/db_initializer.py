"""Initialize a database to handle all of the data for the application."""

import os, json, weakref

from ResearchOS.current_user import CurrentUser
from ResearchOS.config import Config
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.research_object_handler import ResearchObjectHandler

sql_settings_path = os.path.dirname(__file__) + "/config/sql.json"

class DBInitializer():
    """Initializes the database when the Python package is run for the first time using ros-quickstart."""

    def __init__(self, db_file: str = None) -> None:
        """Initialize the database."""
        # Reset everything.
        ResearchObjectHandler.instances = weakref.WeakValueDictionary() # Keep track of all instances of all research objects.
        ResearchObjectHandler.counts = {} # Keep track of the number of instances of each ID.
        ResearchObjectHandler.pool = None
        SQLiteConnectionPool._instance = None

        config = Config()
        if db_file is None:
            db_file = config.db_file
        else:
            config.db_file = db_file
        if os.path.exists(db_file):
            os.remove(db_file)
        with open(sql_settings_path, "r") as file:
            data = json.load(file) 
        intended_tables = data["intended_tables"]        

        self.db_file = db_file
        self.pool = SQLiteConnectionPool()
        ResearchObjectHandler.pool = self.pool
        self.conn = self.pool.get_connection()
        self.create_tables()
        self.check_tables_exist(intended_tables)
        self.pool.return_connection(self.conn)
        self.init_current_user_id()        

    def init_current_user_id(self, user_id: str = "US000000_000"):
        """Initialize the current user ID in the settings table."""
        CurrentUser().set_current_user_id(user_id)

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
        # dataobject_id is just the lowest level data object ID (the lowest level of the address).
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_values (
                        action_id TEXT NOT NULL,
                        dataobject_id TEXT NOT NULL,
                        schema_id TEXT NOT NULL,
                        vr_id TEXT NOT NULL,
                        scalar_value TEXT,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE,
                        FOREIGN KEY (dataobject_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (schema_id) REFERENCES data_address_schemas(schema_id) ON DELETE CASCADE,
                        FOREIGN KEY (VR_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        PRIMARY KEY (dataobject_id, schema_id, vr_id, scalar_value)
                        )""")
        
        # Data addresses. Lists all data addresses for all data.
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_addresses (
                        action_id TEXT,
                        target_object_id TEXT NOT NULL,
                        source_object_id TEXT NOT NULL,
                        schema_id TEXT NOT NULL,
                        FOREIGN KEY (target_object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (source_object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (schema_id) REFERENCES data_address_schemas(schema_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE,
                        PRIMARY KEY (target_object_id, source_object_id, schema_id)
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
        
        # Variable -> DataObjects table. Lists all variables and which data objects they are associated with.
        cursor.execute("""CREATE TABLE IF NOT EXISTS vr_dataobjects (
                        action_id TEXT NOT NULL,
                        vr_id TEXT NOT NULL,
                        dataobject_id TEXT NOT NULL,
                        FOREIGN KEY (vr_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (dataobject_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")


        
if __name__ == '__main__':    
    db = DBInitializer("dev_database.db")