"""Initialize a database to handle all of the data for the application."""

import os, json, weakref, sqlite3

from ResearchOS.current_user import CurrentUser
from ResearchOS.config import Config
from ResearchOS.sqlite_pool import SQLiteConnectionPool
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.action import Action
from ResearchOS.current_user import default_current_user

sql_settings_path = os.sep.join([os.path.dirname(__file__), "config", "sql.json"])

class DBInitializer():
    """Initializes the database when the Python package is run for the first time using ros-quickstart."""

    def __init__(self, main_db_file: str = None, data_db_file: str = None) -> None:
        """Initialize the database."""
        # Reset default attributes.
        ResearchObjectHandler.default_attrs = {} # Reset default attributes dictionary.

        # Reset research object dictionary.
        ResearchObjectHandler.instances = weakref.WeakValueDictionary() # Keep track of all instances of all research objects.        
        ResearchObjectHandler.counts = {} # Keep track of the number of instances of each ID.
        
        # Reset the connection pools for each database.
        SQLiteConnectionPool._instances = {"main": None, "data": None}

        # Remove database files.
        config = Config()
        if main_db_file is None:
            main_db_file = config.db_file
        else:
            config.db_file = main_db_file
        if os.path.exists(main_db_file):
            try:
                os.remove(main_db_file)
            except PermissionError:
                print(f"Could not remove {main_db_file}. Please close the file and try again.")
                return
        if data_db_file is None:
            data_db_file = config.data_db_file
        else:
            config.data_db_file = data_db_file
        if os.path.exists(data_db_file):
            try:
                os.remove(data_db_file)
            except PermissionError:
                print(f"Could not remove {data_db_file}. Please close the file and try again.")
                return
        with open(sql_settings_path, "r") as file:
            sql_settings = json.load(file) 
        intended_tables = sql_settings["intended_tables"]
        intended_tables_data = sql_settings["intended_tables_data"]

        self.db_file = main_db_file
        self.pool = SQLiteConnectionPool(name = "main")

        self.action = Action(name = "initialize database", commit = True, exec = True, force_create = True)
                        
        self.conn = self.action.conn
        self.create_tables()
        self.check_tables_exist(self.conn, intended_tables)
        self.init_current_user_computer_id()
        self.action.add_sql_query("init", "current_user_computer_id_insert", self.action.db_init_params)
        self.action.execute()

        self.data_db_file = data_db_file
        self.pool_data = SQLiteConnectionPool(name = "data")
        self.conn_data = self.pool_data.get_connection()
        self.create_tables_data_db()
        self.check_tables_exist(self.conn_data, intended_tables_data)
        self.pool_data.return_connection(self.conn_data)

    def init_current_user_computer_id(self, user_id: str = default_current_user):
        """Initialize the current user ID in the settings table."""
        CurrentUser(self.action).set_current_user_computer_id(user_id)

    def check_tables_exist(self, conn: sqlite3.Connection, intended_tables: list):
        """Check that all of the tables were created."""        
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        tables = [table[0] for table in tables]
        missing_tables = [table for table in intended_tables if table not in tables]
        if missing_tables:
            print(missing_tables)
            raise Exception("At least one table missing!")
        
    def create_tables_data_db(self):
        """Create the database for the data objects."""
        cursor = self.conn_data.cursor()

        # Data objects data values. Lists all data values for all data objects.
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_values_blob (
                       data_blob_hash TEXT PRIMARY KEY, 
                       data_blob BLOB NOT NULL                        
                        )""")        

    def create_tables(self):
        """Create the database and all of its tables."""
        cursor = self.conn.cursor()

        # Action-tables one-to-many relation table. Lists all actions and which tables they had an effect on.
        # cursor.execute("""CREATE TABLE IF NOT EXISTS action_tables (
        #                 action_id TEXT NOT NULL,
        #                 table_name TEXT NOT NULL,
        #                 PRIMARY KEY (action_id, table_name),
        #                 FOREIGN KEY (action_id) REFERENCES actions(action_id) ON DELETE CASCADE
        #                 )""")

        # Objects table. Lists all research objects in the database, and which action created them.
        cursor.execute("""CREATE TABLE IF NOT EXISTS research_objects (
                        object_id TEXT PRIMARY KEY,
                        action_id_num INTEGER NOT NULL,
                        FOREIGN KEY (action_id_num) REFERENCES actions(action_id_num) ON DELETE CASCADE
                        )""")

        # Actions table. Lists all actions that have been performed, and their timestamps.
        cursor.execute("""CREATE TABLE IF NOT EXISTS actions (
                        action_id_num INTEGER PRIMARY KEY AUTOINCREMENT,
                        action_id TEXT NOT NULL,
                        name TEXT NOT NULL,
                        datetime TEXT NOT NULL,
                        redo_of TEXT,
                        type TEXT NOT NULL,
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
                        action_row_id INTEGER PRIMARY KEY,
                        action_id_num INTEGER NOT NULL,
                        object_id TEXT NOT NULL,
                        attr_id INTEGER NOT NULL,
                        attr_value TEXT,
                        target_object_id TEXT,
                        FOREIGN KEY (attr_id) REFERENCES attributes(attr_id) ON DELETE CASCADE,
                        FOREIGN KEY (object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (target_object_id) REFERENCES research_objects(target_object_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id_num) REFERENCES actions(action_id_num) ON DELETE CASCADE                       
                        )""")
        
        # Data objects data values. Lists all data values for all data objects.
        # dataobject_id is just the lowest level data object ID (the lowest level of the address).
        # Either data blob hash can be null, or scalar_value, but not both or neither.
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_values (
                        action_id_num INTEGER NOT NULL,
                        path_id INTEGER NOT NULL,
                        dynamic_vr_id INTEGER NOT NULL,
                        data_blob_hash TEXT,
                        str_value TEXT,
                        numeric_value INTEGER,
                        is_active INTEGER NOT NULL DEFAULT 1,
                        CHECK (
                            (data_blob_hash IS NOT NULL AND str_value IS NULL AND numeric_value IS NULL) OR
                            (data_blob_hash IS NULL AND NOT (str_value IS NOT NULL AND numeric_value IS NOT NULL))
                        ),
                        FOREIGN KEY (path_id) REFERENCES paths(path_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id_num) REFERENCES actions(action_id_num) ON DELETE CASCADE,
                        FOREIGN KEY (path_id) REFERENCES paths(path_id) ON DELETE CASCADE,                        
                        FOREIGN KEY (dynamic_vr_id) REFERENCES dynamic_vrs(dynamic_vr_id) ON DELETE CASCADE,
                        PRIMARY KEY (action_id_num, path_id, dynamic_vr_id, data_blob_hash, str_value, numeric_value)
                        )""")
        
        # Data addresses. Lists all data addresses for all data.
        cursor.execute("""CREATE TABLE IF NOT EXISTS data_addresses (
                        action_id_num INTEGER NOT NULL,
                        source_object_id TEXT NOT NULL,
                        target_object_id TEXT NOT NULL,
                        FOREIGN KEY (target_object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (source_object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,                        
                        FOREIGN KEY (action_id_num) REFERENCES actions(action_id_num) ON DELETE CASCADE
                        )""")
        
        # Users_Computers table. Maps all users to their computers.
        cursor.execute("""CREATE TABLE IF NOT EXISTS users_computers (
                        action_id_num INTEGER PRIMARY KEY,
                        user_id TEXT NOT NULL,
                        computer_id TEXT NOT NULL,
                        FOREIGN KEY (action_id_num) REFERENCES actions(action_id_num) ON DELETE CASCADE
                        )""")        
        
        # Paths table. Lists all data object paths.
        # "path" column is the JSON'd list of data object names in the path.
        cursor.execute("""CREATE TABLE IF NOT EXISTS paths (
                        path_id INTEGER PRIMARY KEY,
                        action_id_num INTEGER NOT NULL,                        
                        dataobject_id TEXT NOT NULL,
                        is_active INTEGER NOT NULL DEFAULT 1,
                        path TEXT NOT NULL,
                        FOREIGN KEY (dataobject_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (action_id_num) REFERENCES actions(action_id_num) ON DELETE CASCADE
                        )""")
        
        # PipelineObjects Graph table. Lists all pipeline objects and their relationships.
        cursor.execute("""CREATE TABLE IF NOT EXISTS pipelineobjects_graph (
                        edge_id INTEGER NOT NULL,
                        action_id_num INTEGER NOT NULL,
                        input_id INTEGER NOT NULL,
                        output_id INTEGER NOT NULL,
                        is_active INTEGER NOT NULL DEFAULT 1,                        
                        FOREIGN KEY (action_id_num) REFERENCES actions(action_id_num) ON DELETE CASCADE,
                        FOREIGN KEY (input_id) REFERENCES inputs_outputs(id) ON DELETE CASCADE,
                        FOREIGN KEY (output_id) REFERENCES inputs_outputs(id) ON DELETE CASCADE,
                        PRIMARY KEY (action_id_num, edge_id, input_id, output_id, is_active)
                        )""")
        
        # Inputs & outputs table. Lists all inputs & outputs.
        cursor.execute("""CREATE TABLE IF NOT EXISTS inputs_outputs (
                        id INTEGER,
                        is_input INTEGER NOT NULL DEFAULT 1,
                        action_id_num INTEGER NOT NULL,
                        value TEXT,
                        ro_id TEXT NOT NULL,
                        vr_name_in_code TEXT NOT NULL,
                        show INTEGER NOT NULL DEFAULT 0,
                        FOREIGN KEY (action_id_num) REFERENCES actions(action_id_num) ON DELETE CASCADE,
                        FOREIGN KEY (ro_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        PRIMARY KEY (id, is_input, action_id_num, ro_id, vr_name_in_code)
                        )""")
        
        # Inputs_outputs to dynamic_vrs table. Lists all inputs & outputs to dynamic VR's.
        cursor.execute("""CREATE TABLE IF NOT EXISTS inputs_outputs_to_dynamic_vrs (
                        io_dynamic_id INTEGER PRIMARY KEY,
                        action_id_num INTEGER NOT NULL,
                        io_id INTEGER NOT NULL,
                        dynamic_vr_id TEXT NOT NULL,
                        order_num INTEGER NOT NULL,
                        is_active INTEGER NOT NULL DEFAULT 1,    
                        FOREIGN KEY (action_id_num) REFERENCES actions(action_id_num) ON DELETE CASCADE,
                        FOREIGN KEY (io_id) REFERENCES inputs_outputs(id) ON DELETE CASCADE,
                        FOREIGN KEY (dynamic_vr_id) REFERENCES dynamics_vrs(dynamic_vr_id) ON DELETE CASCADE
                        )""")
        
        # run_history table. Lists all actions that occurred during runs.
        cursor.execute("""CREATE TABLE IF NOT EXISTS run_history (
                        action_id_num INTEGER,
                        pl_object_id TEXT NOT NULL,
                        FOREIGN KEY (action_id_num) REFERENCES actions(action_id_num) ON DELETE CASCADE,
                        FOREIGN KEY (pl_object_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        PRIMARY KEY (action_id_num, pl_object_id)
                        )""")
        
        # Dynamic VR's table. Lists all dynamic VR's.
        cursor.execute("""CREATE TABLE IF NOT EXISTS dynamic_vrs (
                        dynamic_vr_id INTEGER PRIMARY KEY,
                        action_id_num INTEGER NOT NULL,
                        vr_id TEXT NOT NULL,
                        pr_id TEXT NOT NULL,
                        FOREIGN KEY (action_id_num) REFERENCES actions(action_id_num) ON DELETE CASCADE,
                        FOREIGN KEY (vr_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        FOREIGN KEY (pr_id) REFERENCES research_objects(object_id) ON DELETE CASCADE,
                        UNIQUE (vr_id, pr_id)
                        )""")