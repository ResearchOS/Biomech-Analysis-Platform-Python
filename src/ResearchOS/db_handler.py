import sqlite3

from config import Config

class DBHandler():
    """Handle database operations."""

    _instance = None

    # def _get_time_ordered_result(unordered_result, action_col_num):
    #     """Return the result ordered by time."""
    #     return sorted(unordered_result, key=lambda x: x[action_col_num])
    
    # def _get_attr_name(attr_id):
    #     """Return the attribute name."""
    #     cursor = Action.conn.cursor()
    #     sqlquery = f"SELECT attr_name FROM attributes WHERE attr_id = '{attr_id}'"
    #     result = cursor.execute(sqlquery).fetchone()
    #     return result[0]
    
class DBHandlerSQLite(DBHandler):
    """Concrete class to interface with the SQLite database.
    Responsible for:
    1. Keeping track of the connection to the database (self.connection)
    2. Providing a cursor object (self.connection.cursor())
    3. Ensuring that only one instance of the class (one connection) is created.
    4. Closing the connection to the database when the object is destroyed."""    

    def __new__(cls, db_file: str) -> "DBHandlerSQLite":
        """Create a new instance of the DBHandlerSQLite."""
        if not cls._instance:
            cls._instance = super(DBHandlerSQLite, cls).__new__(cls)
            cls._instance.connection = sqlite3.connect(db_file)
        return cls._instance

    def __del__(self) -> None:
        """Close the connection to the database."""
        self.connection.close()

    def cursor(self):
        """Return a cursor object."""
        return self.connection.cursor()
    
class DBHandlerMySQL(DBHandler):
    """Concrete class to interface with the MySQL database."""

    def __new__(cls, host: str, user: str, password: str, db_name: str) -> "DBHandlerMySQL":
        """Create a new instance of the DBHandlerMySQL."""
        if not cls._instance:
            cls._instance = super(DBHandlerMySQL, cls).__new__(cls)
            cls._instance.connection = mysql.connector.connect(
                host = host,
                user = user,
                password = password,
                database = db_name
            )
        return super(DBHandlerMySQL, cls).__new__(cls)

    def __del__(self) -> None:
        """Close the connection to the database."""
        self.connection.close()

    def cursor(self):
        """Return a cursor object."""
        return self.connection.cursor()
    
class DBHandlerFactory():
    """Create the appropriate DBHandler concrete class based on the database type in the config."""

    @staticmethod
    def create_db_handler(config: Config) -> DBHandler:
        """Create the appropriate DBHandler concrete class based on the database type in the config."""
        db_type = config.db_type
        if db_type == "sqlite":
            return DBHandlerSQLite(config.db_file)
        elif db_type == "mysql":
            return DBHandlerMySQL(config.db_host, config.db_user, config.db_password, config.db_name)
        else:
            raise ValueError(f"Database type {db_type} not supported.")