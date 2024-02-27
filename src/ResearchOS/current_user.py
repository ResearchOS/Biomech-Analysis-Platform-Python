import datetime
from datetime import timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ResearchOS.action import Action

from ResearchOS.idcreator import IDCreator
# from ResearchOS.sqlite_pool import SQLiteConnectionPool

default_current_user = "default_user"

class CurrentUser():
    """Singular purpose is to return the current user object ID."""

    current_user = ""    

    def __init__(self, action: "Action") -> None:
        """Initialize the CurrentUser class."""
        self.action = action
    
    def get_current_user_id(self, is_init: bool = False) -> str:
        """Get the current user from the actions table in the database.
        Reads the most recent action (by timestamp) and returns the user. User will always exist if an action exists because user is NOT NULL in SQLite table.
        If no actions exist, raise an error."""
        if len(CurrentUser.current_user) > 0:
            # self.pool.return_connection(self.conn)
            return CurrentUser.current_user
        cursor = self.action.conn.cursor()
        sqlquery = "SELECT user FROM actions ORDER BY datetime DESC LIMIT 1"
        result = cursor.execute(sqlquery).fetchone()
        if result is None and is_init:
            return default_current_user
        if result is None and not is_init:
            raise ValueError("current user does not exist because there are no actions")
        # self.pool.return_connection(self.conn)
        CurrentUser.current_user = result[0]
        return CurrentUser.current_user
    
    def set_current_user_id(self, user: str = default_current_user) -> None:
        """Set the current user in the actions table in the database.
        This is the only action that does not affect any other table besides Actions. It is a special case."""        
        action_id = IDCreator(self.action.conn).create_action_id()        
        sqlquery = f"INSERT INTO actions (action_id, user, name, datetime) VALUES (?, ?, ?, ?)"
        params = (action_id, user, self.action.name, datetime.datetime.now(timezone.utc))
        self.action.add_sql_query(sqlquery, params)
        self.action.is_created = True # So the query doesn't happen twice.
        CurrentUser.current_user = user
