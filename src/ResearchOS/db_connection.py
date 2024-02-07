import os, sqlite3

class DBConnection():
    """Store the connection to the database."""

    _instance = None
    
class DBConnectionSQLite(DBConnection):
    """Concrete class to interface with the SQLite database.
    Responsible for:
    1. Keeping track of the connection to the database (self.connection)
    2. Providing a cursor object (self.connection.cursor())
    3. Ensuring that only one instance of the class (one connection) is created.
    4. Closing the connection to the database when the object is destroyed."""    

    def __new__(cls, db_file: str) -> "DBConnectionSQLite":
        """Create a singleton instance of the DBConnectionSQLite."""
        cls.db_file = db_file
        if not cls._instance:
            cls._instance = super(DBConnectionSQLite, cls).__new__(cls)
            if os.path.exists(cls.db_file):
                cls._instance.conn = sqlite3.connect(cls.db_file)
            else:
                raise FileNotFoundError(f"Database file {cls.db_file} does not exist.")
        return cls._instance

    def __del__(self) -> None:
        """Close the connection to the database."""
        self.conn.close()
    
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

    def __del__(self) -> None:
        """Close the connection to the database."""
        self.conn.close()