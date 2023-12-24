"""Comprised of one set of GUI widget states, and one set of research object states."""
import datetime
import sqlite3
from abc import abstractmethod
import uuid

from src.ResearchOS.config import ProdConfig

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

    _db_file: str = ProdConfig.db_file
    # Create a connection to the SQL database.  
    conn = sqlite3.connect(_db_file)
    
    def __init__(self, name: str = None, closeable: bool = True, id: str = None, redo_of: str = None, user_object_id: str = None, timestamp_opened: datetime.datetime = None, timestamp_closed: datetime.datetime = None):   
        if not timestamp_opened:
            timestamp_opened = datetime.datetime.utcnow()
        if not user_object_id:
            user_object_id = get_current_user_object_id()
        # Load the action from the database if an open action exists, otherwise create a new action.
        is_open = Action.is_open()
        if not id:
            if is_open:
                closeable = False
                open_action = Action.get_open()
                for key in open_action.__dict__.keys():
                    setattr(self, key, getattr(open_action, key))
                return
            else:
                # Creating a new action.
                closeable = True
                id = Action._create_uuid() # Making a new action.        
        else:
            # Loading an existing action.
            sqlquery = f"SELECT * FROM actions WHERE action_id = '{id}'"
            result = Action.conn.cursor().execute(sqlquery).fetchone()
            if result is None:
                raise AssertionError(f"Action {id} does not exist.")
            user_object_id = result[1]
            name = result[2]                        
            timestamp_opened = result[3]
            timestamp_closed = result[4]
            redo_of = result[5]
            closeable = True
            if is_open:
                closeable = False

        self.id = id
        self.name = name
        self.timestamp_opened = timestamp_opened
        self.timestamp_closed = timestamp_closed
        self.redo_of = redo_of       
        self.closeable = closeable
        self.user_object_id = user_object_id

        if not is_open:
            self.log()

    ###############################################################################################################################
    #################################################### end of dunder methods ####################################################
    ###############################################################################################################################

    @abstractmethod
    def _create_uuid() -> str:
        """Create the action_id (as uuid.uuid4()) for the action."""
        import uuid
        is_unique = False
        cursor = Action.conn.cursor()
        while not is_unique:
            uuid_out = str(uuid.uuid4()) # For testing dataset creation.            
            sql = f'SELECT action_id FROM actions WHERE action_id = "{uuid_out}"'
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) == 0:
                is_unique = True
        return uuid_out
    
    @abstractmethod
    def _is_action_id(uuid: str) -> bool:
        """Check if a string is a valid UUID."""
        import uuid as uuid_module
        try:
            uuid_module.UUID(str(uuid))
        except ValueError:
            return False
        return True    
    
    @abstractmethod
    def set_current_action(action: "Action") -> None:
        """Set the current action."""        
        cursor = Action.conn.cursor()
        setting_name = 'current_action_id'
        sqlquery = f"INSERT INTO settings (user_object_id, setting_name, setting_value) VALUES ('{action.user_object_id}', '{setting_name}', '{action.id}')"
        cursor.execute(sqlquery)
        Action.conn.commit()

    @abstractmethod
    def load(id: str = None):
        """Load an action from file given a timestamp or id."""        
        return Action(id = id)
    
    @abstractmethod
    def previous(id: str = None, current_time: datetime.datetime = None) -> "Action":
        """Get the ID and all contents of the most recently performed action."""
        cursor = Action.conn.cursor()
        if not current_time:
            current_time = datetime.datetime.utcnow()        
        if id:
            action = Action.load(id = id)
            current_time = action.timestamp_opened
        sqlquery = f"SELECT action_id FROM actions WHERE timestamp_opened <= '{str(current_time)}' ORDER BY timestamp_opened DESC LIMIT 1"
        result = cursor.execute(sqlquery).fetchone()
        if result is None:
            return None
        id = result[0]
        return Action.load(id = id)
    
    @abstractmethod
    def get_next(id: str = None, current_time: datetime.datetime = None) -> "Action":
        """Get the ID and all contents of the next action."""
        cursor = Action.conn.cursor()
        if not current_time:
            current_time = datetime.datetime.utcnow()        
        if id:
            action = Action.load(id = id)
            current_time = action.timestamp_opened
        sqlquery = f"SELECT action_id FROM actions WHERE timestamp_opened >= '{str(current_time)}' ORDER BY timestamp ASC LIMIT 1"
        result = cursor.execute(sqlquery).fetchone()
        if result is None:
            return None
        id = result[0]
        return Action.load(id = id)
    
    @abstractmethod
    def is_open() -> bool:
        """Return True if there is an open action, False otherwise."""
        cursor = Action.conn.cursor()
        sqlquery = "SELECT action_id FROM actions WHERE timestamp_closed IS NULL"
        result = cursor.execute(sqlquery).fetchone()
        if result is None or len(result) == 0:
            return False
        if len(result) > 1:
            raise AssertionError("There are multiple open actions.")
        
        return True

    @abstractmethod
    def open(name: str) -> "Action":
        """Creates a new action and logs it to the database, opening it for use. All operations performed after this will be logged to this action until it is closed."""
        if Action.is_open():
            return Action.get_open(closeable = False)
        
        action = Action(name = name, closeable = True)
        action.log() # Puts the action in the database.
        Action.set_current_action(action = action)
        return action
    
    @abstractmethod
    def get_open(closeable: bool = False) -> "Action":
        """Return the open action."""
        cursor = Action.conn.cursor()
        sqlquery = "SELECT action_id FROM actions WHERE timestamp_closed IS NULL"
        result = cursor.execute(sqlquery).fetchone()
        if len(result) == 0:
            return None
        id = result[0]
        return Action.load(id = id)
    
    ###############################################################################################################################
    #################################################### end of abstract methods ##################################################
    ###############################################################################################################################

    def execute(self) -> None:
        """Execute the action, causing the current state of the referenced widgets and research objects to be the state in this Action."""        
        # Create a new action, where "redo_of" is set to self.id.
        action = Action(name = self.name, closeable = True, redo_of = self.id)
        action.open() # Opens the action.
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

    def redo(self) -> None:
        """Execute the action, causing the current state of the referenced widgets and research objects to be the state in this Action."""        
        # Create a new action, where "redo_of" is set to self.id.
        #TODO: This should be callable from anywhere, not just from self. Needs to reference the current_action_id for that user.
        next_action = Action.get_next(id = self.id)
        if next_action is None:
            return
        Action.set_current_action(action = next_action)
        next_action.execute()

    def undo(self) -> None:
        """Undo the action, causing the current state of the referenced widgets and research objects to be the state of the previous Action."""
        # Log the action to the database.
        #TODO: This should be callable from anywhere, not just from self. Needs to reference the current_action_id for that user.
        prev_action = Action.previous(id = self.id)
        if prev_action is None:
            return
        Action.set_current_action(action = prev_action)
        prev_action.execute()    

    def _get_rows_of_action(action_id: str, table_name: str) -> list:
        """Get the rows of a table that were created by an action."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT * FROM {table_name} WHERE action_id = '{action_id}'"
        return cursor.execute(sqlquery).fetchall()    
    
    def log(self):
        """Log the action to the database."""        
        Action.conn.cursor().execute("INSERT INTO actions (action_id, user_object_id, name, timestamp_opened, timestamp_closed, redo_of) VALUES (?, ?, ?, ?, ?, ?)", (self.id, self.user_object_id, self.name, self.timestamp_opened, self.timestamp_closed, self.redo_of))        
        Action.conn.commit()
    
    def close(self):
        """Closeable only if called from inside the same method that opened it."""        
        if not self.closeable:
            return
        cursor = Action.conn.cursor()
        self.timestamp_closed = datetime.datetime.utcnow()
        sqlquery = f"UPDATE actions SET timestamp_closed = '{str(self.timestamp_closed)}' WHERE action_id = '{self.id}'"
        cursor.execute(sqlquery)
        Action.conn.commit()    

if __name__=="__main__":    
    action = Action("Test Action")
    action.close()