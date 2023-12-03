"""Comprised of one set of GUI widget states, and one set of research object states."""
import datetime
import sqlite3
from abc import abstractmethod
import uuid

def get_current_user_object_id() -> str:
    """Get the ID of the current user."""
    cursor = Action.conn.cursor()
    sqlquery = "SELECT current_user_object_id FROM current_user"
    result = cursor.execute(sqlquery).fetchone()
    if result is None or len(result) == 0:
        return None
    if len(result) > 1:
        raise AssertionError("There are multiple current users.")
    return result[0]

def set_current_user_object_id(user_object_id: str) -> None:
    """Set the ID of the current user."""
    cursor = Action.conn.cursor()
    sqlquery = f"INSERT INTO current_user (current_user_object_id) VALUES ('{user_object_id}')"
    cursor.execute(sqlquery)
    Action.conn.commit()

class Action():    

    # Create a connection to the SQL database.  
    conn = sqlite3.connect("./SQL/database.db")
    
    def __init__(self, name: str = None, closeable: bool = True, id: str = None, redo_of: str = None, user_object_id: str = None, timestamp_opened: datetime.datetime = None, timestamp_closed: datetime.datetime = None):   
        if not id:
            # Creating a new action.
            id = str(uuid.uuid4()) # Making a new action.
            if not timestamp_opened:
                timestamp_opened = datetime.datetime.utcnow()
            if not user_object_id:
                user_object_id = get_current_user_object_id()
        else:
            # Loading an existing action.
            sqlquery = f"SELECT * FROM actions WHERE action_id = '{id}'"
            result = Action.conn.cursor().execute(sqlquery).fetchone()
            name = result[1]
            user_object_id = result[2]
            name =  result[3]
            timestamp_opened = result[4]
            timestamp_closed = result[5]
            redo_of = result[6]

        self.id = id
        self.name = name
        self.timestamp_opened = timestamp_opened
        self.timestamp_closed = timestamp_closed
        self.redo_of = redo_of       
        self.closeable = closeable
        self.user_object_id = user_object_id

    def redo(self) -> None:
        """Execute the action, causing the current state of the referenced widgets and research objects to be the state in this Action."""        
        # Create a new action, where "redo_of" is set to self.id.
        action = Action(name = self.name, closeable = True, redo_of = self.id)
        action.log() # Opens the action.
        cursor = Action.conn.cursor()
        table_names = ["data_values", "research_object_attributes", "data_address_schemas", "data_addresses"]
        column_labels_list = [["action_id", "address_id", "schema_id", "VR_id", "PR_id", "scalar_value"], 
                         ["action_id", "object_id", "attr_id", "attr_value", "child_of"],
                         ["schema_id", "level_name", "level_index", "action_id", "dataset_id"],
                         ["address_id", "schema_id", "level0", "level1", "level2", "level3", "level4", "level5", "level6", "level7", "level8", "level9", "action_id"]]
        for idx, table_name in enumerate(table_names):
            labels = column_labels_list[idx]
            rows = Action._get_rows_of_action(action_id = self.id, table_name = table_name)
            labels_formatted = ", ".join(labels)
            action_id_index = labels.index("action_id")
            for row in rows:                
                row[action_id_index] = action.id # Update the action_id to the new action.
                row_formatted = ", ".join(row)
                sqlquery = f"INSERT INTO {table_name} ({labels_formatted}) VALUES ({row_formatted})"
                cursor.execute(sqlquery, row)
        
        Action.conn.commit()
        action.close()


    def undo(self):
        """Undo the action, causing the current state of the referenced widgets and research objects to be the state of the previous Action."""
        # Log the action to the database.
        prev_action = Action.get_most_recent(id = self.id)
        prev_action.redo()

    def _get_rows_of_action(action_id: str, table_name: str) -> list:
        """Get the rows of a table that were created by an action."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT * FROM {table_name} WHERE action_id = '{action_id}'"
        return cursor.execute(sqlquery).fetchall()

    @abstractmethod
    def load(id: str = None):
        """Load an action from file given a timestamp or id."""
        cursor = Action.conn.cursor()

        sqlquery = f"SELECT * FROM actions WHERE id = '{id}'"
        result = cursor.execute(sqlquery).fetchone()
        if len(result) == 0:
            raise ValueError("No action with that ID exists.")
        id = result[0]
        user_object_id = result[1]
        name =  result[2]
        timestamp_opened = result[3]
        timestamp_closed = result[4]
        redo_of = result[5]
                    
        return Action(name = name, id = id, user_object_id = user_object_id, timestamp_opened = timestamp_opened, timestamp_closed = timestamp_closed, redo_of = redo_of)

    @abstractmethod
    def get_most_recent(id: str = None, current_time: datetime.datetime = None) -> "Action":
        """Get the ID and all contents of the most recently performed action."""
        cursor = Action.conn.cursor()
        if not current_time:
            current_time = datetime.datetime.utcnow()        
        if id:
            action = Action.load(id = id)
            current_time = action.timestamp_opened
        sqlquery = f"SELECT action_id FROM actions WHERE timestamp_opened <= '{str(current_time)}' ORDER BY timestamp DESC LIMIT 1"
        result = cursor.execute(sqlquery).fetchone()
        if result is None:
            return None
        id = result[0]
        return Action.load(id = id)
    
    @abstractmethod
    def is_open() -> bool:
        """Return True if there is an open action, False otherwise."""
        cursor = Action.conn.cursor()
        sqlquery = "SELECT action_id FROM actions WHERE timestamp_closed = NULL"
        result = cursor.execute(sqlquery).fetchone()
        if result is None or len(result) == 0:
            return False
        if len(result) > 1:
            raise AssertionError("There are multiple open actions.")
        
        return True

    @abstractmethod
    def open(name: str, closeable: bool = False) -> "Action":
        """Creates a new action and logs it to the database, opening it for use. All operations performed after this will be logged to this action until it is closed."""
        if Action.is_open():
            return Action.get_open(closeable = closeable)
        
        action = Action(name = name, closeable = closeable)
        action.log() # Puts the action in the database.
        return action
    
    def log(self):
        """Log the action to the database."""
        Action.conn.cursor().execute("INSERT INTO actions (action_id, user_object_id, name, timestamp_opened, timestamp_closed, redo_of) VALUES (?, ?, ?, ?, ?, ?)", (self.id, self.user_object_id, self.name, self.timestamp_opened, self.timestamp_closed, self.redo_of))        
        Action.conn.commit()
    
    def close(self):
        """Closeable only if called from inside the same method that opened it."""
        from research_object import ResearchObject
        if not self.closeable:
            return
        cursor = Action.conn.cursor()
        self.timestamp_closed = datetime.datetime.utcnow()
        sqlquery = f"UPDATE actions SET timestamp_closed = '{str(self.timestamp_closed)}' WHERE action_id = '{self.id}'"
        cursor.execute(sqlquery)

        # Set the current action as the one that is being redone.
        sqlquery = f"INSERT INTO settings (user_object_id, setting_name, setting_value) VALUES ('{self.user_object_id}', {ResearchObject._get_attr_id('current_action_id')}, '{self.id}')"
        cursor.execute(sqlquery)
        Action.conn.commit()

    @abstractmethod
    def get_open(closeable: bool = True) -> "Action":
        """Return the open action."""
        cursor = Action.conn.cursor()
        sqlquery = "SELECT id FROM actions WHERE timestamp IS NULL"
        result = cursor.execute(sqlquery).fetchone()
        if len(result) == 0:
            return None
        id = result[0]
        return Action.load(id = id)

if __name__=="__main__":
    action = Action("Test Action")
    action.close()