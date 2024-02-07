from ResearchOS.action import Action
from ResearchOS.db_connection import DBConnection

class DBHandler():
    """Handle database interactions."""

    def __init__(self, db: DBConnection) -> None:
        """Initialize the DBHandler."""
        self.conn = db.conn

    def isolate_most_recent(self, result: list, action_col_num: int, duplicate_col_num: int) -> list:
        """Return the result with only the most recent entries (no overwritten attributes)."""
        # 1. Sort the result by the action column with most recent first.
        cursor = self.conn.cursor()
        user_id = self.get_current_user_object_id()
        sqlquery = f"SELECT action_id FROM actions WHERE user_object_id = '{user_id}' ORDER BY timestamp DESC"
        result = cursor.execute(sqlquery).fetchall()
        sorted_result = sorted(result, key=lambda x: x[action_col_num])
        # 2. Iterate through the result and add the most recent entries to a new list. 
        # If a duplicate is found in the duplicate_col_num, skip it.

    def get_current_user_object_id(self) -> str:
        """Get the current user object ID from the database."""
        cursor = self.conn.cursor()
        
    
    def set_current_user_object_id(self, user_object_id: str) -> None:
        """Set the current user object ID in the database."""
        cursor = self.conn.cursor()
        sqlquery = f"INSERT INTO actions (action_id, user_object_id, name, timestamp, redo_of) VALUES ('{action_id}', '{user_object_id}', '{name}', '{timestamp}', '{redo_of}')"
        cursor.execute(sqlquery)
        self.conn.commit()
        

    def add_setting(self, setting: str, value: str) -> None:
        """Add a setting to the settings table."""
        cursor = self.conn.cursor()
        action = Action(name = "create setting")
        sqlquery = f"INSERT INTO settings (action_id name, value) VALUES ('{setting}', '{value}')"
        action.add_sql_query(sqlquery)
        action.execute()

    def get_setting(self, setting: str) -> str:
        """Get a setting from the settings table."""
        cursor = self.conn.cursor()
        sqlquery = f"SELECT action_id, name, value FROM settings WHERE setting = '{setting}'"
        result = cursor.execute(sqlquery).fetchall()
        result = self.isolate_most_recent(result, 1, )
        if len(result) == 0:
            raise ValueError(f"Setting {setting} does not exist.")
        return result[0][0]