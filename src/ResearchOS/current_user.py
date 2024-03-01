from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ResearchOS.action import Action

from ResearchOS.idcreator import IDCreator
from ResearchOS.get_computer_id import COMPUTER_ID
from ResearchOS.sql.sql_joiner_most_recent import sql_joiner_most_recent

default_current_user = "default_user"

class CurrentUser():
    """Singular purpose is to return the current user object ID."""

    current_user = ""    

    def __init__(self, action: "Action") -> None:
        """Initialize the CurrentUser class."""
        self.action = action
    
    def get_current_user_id(self) -> str:
        """Get the current user from the actions table in the database.
        Reads the most recent action (by timestamp) and returns the user. User will always exist if an action exists because user is NOT NULL in SQLite table.
        If no actions exist, raise an error."""
        if len(CurrentUser.current_user) > 0:
            return CurrentUser.current_user
        cursor = self.action.conn.cursor()

        sqlquery_raw = "SELECT user_id FROM users_computers WHERE computer_id = ?"
        sqlquery = sql_joiner_most_recent(sqlquery_raw)
        result = cursor.execute(sqlquery, (COMPUTER_ID,)).fetchone()
        if result is None:
            raise ValueError("current user does not exist because there are no users for this computer")

        CurrentUser.current_user = result[0]
        return CurrentUser.current_user
    
    def set_current_user_computer_id(self, user: str = default_current_user) -> None:
        """Set the current user in the user_computer_id table in the database.
        This is the only action that does not affect any other table besides Actions. It is a special case."""        
        action_id = IDCreator(self.action.conn).create_action_id()
        params = (action_id, user, COMPUTER_ID)
        self.action.db_init_params = params
        CurrentUser.current_user = user
        CurrentUser.computer_id = COMPUTER_ID
