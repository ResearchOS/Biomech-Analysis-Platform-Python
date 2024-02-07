import sqlite3

class CurrentUser():
    """Singular purpose is to return the current user object ID."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Initialize the CurrentUser class."""
        self.conn = conn
    
    def get_current_user_object_id(self) -> str:
        """Get the current user object ID from the database."""
        cursor = self.conn.cursor()
        sqlquery = "SELECT value FROM settings WHERE setting = 'current_user_object_id'"
        result = cursor.execute(sqlquery).fetchone()
        if result is None:
            raise ValueError("current_user_object_id setting does not exist.")
        return result[0]
        