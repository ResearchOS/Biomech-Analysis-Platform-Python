"""Comprised of one set of GUI widget states, and one set of research object states."""
import datetime
import sqlite3
from abc import abstractmethod
import uuid as uid
import traceback

# from widget_objects.widgets_state import WState
# from research_objects_state import ROState

class Action():    

    # Create a connection to the SQL database.  
    conn = sqlite3.connect("database.db")
    
    def __init__(self, name: str, uuid: str = None):   
        self.name = name     
        self.closeable = True                 
        if not Action.is_open():            
            self.closeable = False                    
        if not uuid:
            uuid = str(uid.uuid4())
        self.uuid = uuid
        self.timestamp = None # Timestamp of None means that the Action is "open".
        sqlquery = f"INSERT INTO Actions (uuid, timestamp, name) VALUES ('{self.uuid}', '{self.timestamp}', '{self.name}')"
        # self.conn.cursor().execute(sqlquery)
        self.previous = Action.get_most_recent()

    def execute(self):
        """Execute the action, causing the current state of the referenced widgets and research objects to be the state in this Action."""
        # Log the action to the database.
        pass

    @abstractmethod
    def load(time: str = None, uuid: str = None):
        """Load an action from file given a timestamp or UUID."""
        cursor = Action.conn.cursor()
        if (time is None and uuid is None) or (time is not None and uuid is not None):
            raise ValueError("Either one of a timestamp or a UUID must be provided, but not both.")
        
        if time:
            sqlquery = f"SELECT uuid, name, timestamp FROM Actions WHERE timestamp <= '{str(time)}' ORDER BY timestamp DESC LIMIT 1"            
        elif uuid:
            sqlquery = f"SELECT uuid, name, timestamp FROM Actions WHERE uuid = '{uuid}'"
        # result = cursor.execute(sqlquery).fetchone()
        result = []
        if len(result) == 0:
            return None
        uuid = result[0]
        name = result[1] 
        timestamp = result[2]
                        
        action = Action(name = name, uuid = uuid)                
        action.timestamp = timestamp
        return action

    @abstractmethod
    def get_most_recent():
        """Get the ID and all contents of the most recently performed action."""
        current_time = datetime.datetime.utcnow()
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT uuid FROM Actions WHERE timestamp <= '{str(current_time)}' ORDER BY timestamp DESC LIMIT 1"
        # result = cursor.execute(sqlquery).fetchone()
        result = []
        if len(result) == 0:
            return None
        uuid = result[0]
        return Action.load(uuid = uuid)
    
    @abstractmethod
    def is_open() -> bool:
        """Return True if there is an open action, False otherwise."""
        cursor = Action.conn.cursor()
        sqlquery = "SELECT uuid FROM Actions WHERE timestamp IS NULL"
        # result = cursor.execute(sqlquery).fetchone()
        result = []
        if len(result) == 0:
            return False
        try:
            assert(len(result) > 1)
        except AssertionError:
            raise AssertionError("There are multiple open actions.")
        return True

    @abstractmethod
    def new(name: str) -> "Action":
        return Action(name = name)
    
    def close(self):
        """Closeable only if called from inside the same method that opened it."""
        if not self.closeable:
            return
        cursor = self.conn.cursor()
        self.timestamp = datetime.datetime.utcnow()
        sqlquery = f"UPDATE Actions SET timestamp = '{str(self.timestamp)}' WHERE uuid = '{self.uuid}'"
        cursor.execute(sqlquery)

    @abstractmethod
    def get_open():
        """Get the UUID of the open action."""
        cursor = Action.conn.cursor()
        sqlquery = "SELECT uuid FROM Actions WHERE timestamp IS NULL"
        # result = cursor.execute(sqlquery).fetchone()
        result = []
        if len(result) == 0:
            return None
        uuid = result[0]
        return Action.load(uuid = uuid)

if __name__=="__main__":
    action = Action("Test Action")
    action.close()