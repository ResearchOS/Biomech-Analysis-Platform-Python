import os, sqlite3, datetime, weakref

class DBConnection():
    """Store the connection to the database."""

    conn_dict = {}

def adapt_datetime_iso(datetime: datetime.datetime) -> str:
    """Adapt datetime.datetime to timezone-naive ISO 8601 date."""
    return datetime.isoformat()

def convert_datetime_iso(iso_date) -> datetime.datetime:
    """Convert timezone-naive ISO 8601 date to datetime.datetime."""
    return datetime.datetime.fromisoformat(iso_date.decode())

sqlite3.register_adapter(datetime.datetime, adapt_datetime_iso)
sqlite3.register_converter("datetime", convert_datetime_iso)
    
class DBConnectionSQLite(DBConnection):
    """Concrete class to interface with the SQLite database.
    Responsible for:
    1. Keeping track of the connection to the database (self.connection)
    2. Providing a cursor object (self.connection.cursor())
    3. Ensuring that only one instance of the class (one connection) is created.
    4. Closing the connection to the database when the object is destroyed."""    

    # def __new__(cls, db_file: str, singleton: bool) -> "DBConnectionSQLite":
    #     """Create a singleton instance of the DBConnectionSQLite."""
    #     cls.db_file = db_file
    #     if not singleton:
    #         cls._instance = None
    #     if not cls._instance:
    #         cls._instance = super(DBConnectionSQLite, cls).__new__(cls)
    #         if os.path.exists(cls.db_file):
    #             # Details on detect_types: https://docs.python.org/3/library/sqlite3.html#sqlite3.PARSE_DECLTYPES
    #             cls._instance.conn = sqlite3.connect(cls.db_file, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    #         else:
    #             raise FileNotFoundError(f"Database file {cls.db_file} does not exist.")
    #     return cls._instance

    def __new__(cls, db_file: str, singleton: bool) -> "DBConnectionSQLite":
        if not cls.conn_dict:
            if os.path.exists(db_file):
                cls.conn_dict["conn"] = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
            else:
                raise FileNotFoundError(f"Database file {db_file} does not exist.")
        return super(DBConnectionSQLite, cls).__new__(cls)
    
    def __init__(self):
        self.conn = self.conn_dict["conn"]
    
    # def __init__(self, db_file: str, singleton: bool) -> None:
    #     """Initialize the database connection."""
    #     # if not singleton:
    #     #     if self.conn:
    #     #         self.conn.close()
    #     #     self.conn = None
    #     # if self.conn is not None:
    #     #     return
    #     if self.conn:
    #         self.conn.close()
    #     if os.path.exists(db_file):
    #         self.conn = sqlite3.connect(db_file, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    #     else:
    #         raise FileNotFoundError(f"Database file {db_file} does not exist.")
        
    def close(self):
        """Close the connection to the database."""
        self.conn.close()

    # def __del__(self) -> None:
    #     """Close the connection to the database."""
    #     self.conn.close()
    
class DBConnectionMySQL(DBConnection):
    """Concrete class to interface with the MySQL database."""

    def __new__(cls, host: str, user: str, password: str, db_name: str) -> "DBConnectionMySQL":
        """Create a new instance of the DBConnectionMySQL."""
        if not cls._instance:
            cls._instance = super(DBConnectionMySQL, cls).__new__(cls)
            cls._instance.conn = mysql.connector.connect(
                host = host,
                user = user,
                password = password,
                database = db_name
            )
        return super(DBConnectionMySQL, cls).__new__(cls)

    # def __del__(self) -> None:
    #     """Close the connection to the database."""
    #     self.conn.close()
