"""Initialize a database to handle all of the data for the application."""

import sqlite3, os, json
import ResearchOS as ros
from ResearchOS.action import Action
from ResearchOS.research_object import DEFAULT_USER_ID


sql_settings_path = os.path.abspath("src/ResearchOS/config/sql.json")
with open(sql_settings_path, "r") as file:
    data = json.load(file)
intended_tables = data["intended_tables"]

class DBInitializer():
    """Initializes the database when the Python package is run for the first time using ros-quickstart."""

    def __init__(self, db_file: str) -> None:
        """Initialize the database."""
        self.db_file = db_file
        self.conn = sqlite3.connect(db_file)
        self.create_tables()
        self.check_tables_exist(intended_tables)

    def check_tables_exist(self, intended_tables: list):
        """Check that all of the tables were created."""        
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        table_names = sorted([table[0] for table in tables])
        intended_tables = sorted(intended_tables)
        missing_tables = [table for table in intended_tables if table not in table_names]
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

        # Objects table. Lists all research objects in the database.
        cursor.execute("""CREATE TABLE IF NOT EXISTS research_objects (
                        object_id TEXT PRIMARY KEY,
                        action_id TEXT NOT NULL,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE
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
                        levels_ordered TEXT NOT NULL,
                        FOREIGN KEY (dataset_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE
                        )""")

    

# class DBInitializer():
#     def __init__(self, remove: bool = True):     
#         config = ros.Config()   
#         db_file = config.db_file
#         Action.conn.close()
#         self.remove = remove
#         if os.path.exists(db_file) and self.remove:
#             os.remove(db_file)
#         Action.conn = sqlite3.connect(db_file)
#         self._conn = Action.conn
#         # self._conn = sqlite3.connect(db_file)   
#         full_file = os.path.abspath(db_file)         
#         folder = os.path.dirname(full_file)
#         # os.chmod(full_file, 0o755)
#         # os.chmod(folder, 0o755)
#         self.create_database()
#         self._conn.commit()
#         self.check_tables_exist()
#         # self.init_current_user_id()
#         if not self.remove:
#             self._conn.cursor().close()
#             self._conn.close()
#             Action.conn = sqlite3.connect(db_file)
#             self._conn = Action.conn

    
            
#     def init_current_user_id(self, user_id: str = DEFAULT_USER_ID):
#         """Initialize the current user ID."""
#         from ResearchOS.action import Action
#         cursor = self._conn.cursor()
#         sqlquery = f"INSERT INTO research_objects (object_id) VALUES ('{user_id}')"
#         cursor.execute(sqlquery)
#         self._conn.commit()
#         action_id = Action._create_uuid()
#         sqlquery = f"INSERT INTO current_user (action_id, current_user_object_id) VALUES ('{action_id}', '{user_id}')"        
#         cursor.execute(sqlquery)
#         sqlquery = f"INSERT INTO actions (action_id, user_object_id, name, timestamp) VALUES ('{action_id}', '{user_id}', 'Initialize current user', '{datetime.datetime.now(datetime.UTC)}')"
#         cursor.execute(sqlquery)
#         # Put the attributes into the research objects attributes table.
#         self._conn.commit()             

#     def create_database(self):


        
if __name__ == '__main__':    
    db = DBInitializer()