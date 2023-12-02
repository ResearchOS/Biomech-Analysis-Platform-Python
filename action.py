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

class Action():    

    # Create a connection to the SQL database.  
    conn = sqlite3.connect("./SQL/database.db")
    
    def __init__(self, name: str = None, closeable: bool = True, id: str = None):   
        if not id:
            id = str(uuid.uuid4()) # Making a new action.
            timestamp_opened = datetime.datetime.utcnow()
            timestamp_closed = None
            redo_of = None
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

    def execute(self):
        """Execute the action, causing the current state of the referenced widgets and research objects to be the state in this Action."""
        # Log the action to the database.
        pass

    @abstractmethod
    def load(id: str = None):
        """Load an action from file given a timestamp or id."""
        cursor = Action.conn.cursor()

        sqlquery = f"SELECT * FROM actions WHERE id = '{id}'"
        result = cursor.execute(sqlquery).fetchone()
        if len(result) == 0:
            raise ValueError("No action with that ID exists.")
        id = result[0]
        name = result[1] 
                        
        action = Action(name = name, id = id)                
        return action

    @abstractmethod
    def get_most_recent(id: str = None, time: str = None):
        """Get the ID and all contents of the most recently performed action."""
        if not id and not time:
            return Action.get_current()
        current_time = datetime.datetime.utcnow()
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT action_id FROM actions WHERE timestamp <= '{str(current_time)}' ORDER BY timestamp DESC LIMIT 1"
        result = cursor.execute(sqlquery).fetchone()
        result = []
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
    def open(name: str) -> "Action":
        if Action.is_open():
            return Action.get_open(closeable = False)
        
        action = Action(name = name, closeable = True)
        Action.conn.cursor().execute("INSERT INTO actions (action_id, user_object_id, name, timestamp_opened, timestamp_closed, redo_of) VALUES (?, ?, ?, ?, ?, ?)", (action.id, action.user_object_id, action.name, action.timestamp_opened, action.timestamp_closed, action.redo_of))        
        Action.conn.commit()
        return action
    
    def close(self):
        """Closeable only if called from inside the same method that opened it."""
        if not self.closeable:
            return
        cursor = self.conn.cursor()
        self.timestamp = datetime.datetime.utcnow()
        sqlquery = f"UPDATE actions SET timestamp_closed = '{str(self.timestamp)}' WHERE action_id = '{self.id}'"
        cursor.execute(sqlquery)

    @abstractmethod
    def get_open(closeable: bool = True) -> "Action":
        """Return the open action."""
        cursor = Action.conn.cursor()
        sqlquery = "SELECT id FROM actions WHERE timestamp IS NULL"
        result = cursor.execute(sqlquery).fetchone()
        result = []
        if len(result) == 0:
            return None
        id = result[0]
        return Action.load(id = id)

if __name__=="__main__":
    action = Action("Test Action")
    action.close()