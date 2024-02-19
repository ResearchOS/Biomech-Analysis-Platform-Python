import sqlite3, datetime
from datetime import timezone

from ResearchOS.idcreator import IDCreator
from ResearchOS.sqlite_pool import SQLiteConnectionPool

class CurrentUser():
    """Singular purpose is to return the current user object ID."""

    def __init__(self) -> None:
        """Initialize the CurrentUser class."""
        pool = SQLiteConnectionPool()            
        conn = pool.get_connection()
        self.pool = pool
        self.conn = conn
    
    def get_current_user_id(self) -> str:
        """Get the current user from the actions table in the database.
        Reads the most recent action (by timestamp) and returns the user. User will always exist if an action exists because user is NOT NULL in SQLite table.
        If no actions exist, raise an error."""        
        cursor = self.conn.cursor()
        sqlquery = "SELECT user FROM actions ORDER BY datetime DESC LIMIT 1"
        result = cursor.execute(sqlquery).fetchone()
        if result is None:
            raise ValueError("current user does not exist because there are no actions")
        self.pool.return_connection(self.conn)
        return result[0]
    
    def set_current_user_id(self, user: str) -> None:
        """Set the current user in the actions table in the database.
        This is the only action that does not affect any other table besides Actions. It is a special case."""
        cursor = self.conn.cursor()
        action_id = IDCreator().create_action_id()
        name = "Set current user"
        sqlquery = f"INSERT INTO actions (action_id, user, name, datetime) VALUES ('{action_id}', '{user}', '{name}', '{datetime.datetime.now(timezone.utc)}')"
        cursor.execute(sqlquery)
        self.conn.commit()
        self.pool.return_connection(self.conn)
