"""Comprised of one set of GUI widget states, and one set of research object states."""
import datetime
import sqlite3
from abc import abstractmethod
import uuid

# import os, sys
# PROJECT_ROOT = os.path.abspath(os.path.join(
#                   os.path.dirname(__file__), 
#                   os.pardir)
# )
# sys.path.append(PROJECT_ROOT)

from src.ResearchOS.config import ProdConfig

class Action():

    _db_file: str = ProdConfig.db_file
    # Create a connection to the SQL database.  
    conn = sqlite3.connect(_db_file)
    
    def __init__(self, name: str = None, id: str = None, redo_of: str = None, user_object_id: str = None, timestamp: datetime.datetime = None):                           
        from src.ResearchOS.user import User
        cursor = Action.conn.cursor()
        if not id:
            id = Action._create_uuid()            
            if not timestamp:
                timestamp = datetime.datetime.utcnow()
            if not user_object_id:
                user_object_id = User.get_current_user_object_id()
            # Do not commit here! When the action is executed then the action and the other db changes will be committed.
            cursor.execute("INSERT INTO actions (action_id, user_object_id, name, timestamp, redo_of) VALUES (?, ?, ?, ?, ?)", (id, user_object_id, name, timestamp, redo_of))
        else:
            # Loading an existing action.
            sqlquery = f"SELECT * FROM actions WHERE action_id = '{id}'"
            result = cursor.execute(sqlquery).fetchone()            
            if result is None:
                raise AssertionError(f"Action {id} does not exist.")
            if len(result) > 1:
                raise AssertionError(f"Action {id} is not unique.")
            user_object_id = result[1]
            name = result[2]                        
            timestamp = result[3]            
            redo_of = result[4]

        self.id = id
        self.name = name
        self.timestamp = timestamp        
        self.redo_of = redo_of               
        self.user_object_id = user_object_id
        self.sql_queries = []

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
    def get_current_action() -> "Action":
        """Get the action that is set as 'current'."""
        cursor = Action.conn.cursor()  
        setting_name = 'current_action_id'
        sqlquery = f"SELECT setting_value FROM settings WHERE setting_name = {setting_name}"
        result = cursor.execute(sqlquery).fetchone()
        if result is None:
            return None
        id = result[0]
        return Action(id = id)
    
    ###############################################################################################################################
    #################################################### end of abstract methods ##################################################
    ###############################################################################################################################

    def next(self) -> "Action":
        """Get the next action after this action."""
        cursor = Action.conn.cursor()
        id = self.id
        timestamp = self.timestamp
        sqlquery = f"SELECT action_id FROM actions WHERE timestamp <= '{str(timestamp)}' ORDER BY timestamp DESC LIMIT 1"
        result = cursor.execute(sqlquery).fetchone()
        if result is None:
            return None
        id = result[0]
        return Action(id = id)

    def previous(self) -> "Action":
        """Get the previous action before this action."""
        cursor = Action.conn.cursor()
        id = self.id
        timestamp = self.timestamp
        sqlquery = f"SELECT action_id FROM actions WHERE timestamp >= '{str(timestamp)}' ORDER BY timestamp ASC LIMIT 1"
        result = cursor.execute(sqlquery).fetchone()
        if result is None:
            return None
        id = result[0]
        return Action(id = id)
    
    def add_sql_query(self, sqlquery: str) -> None:
        """Add a sql query to the action."""
        self.sql_queries.append(sqlquery)

    def execute(self, commit: bool = True) -> None:
        """Run all of the sql queries in the action."""
        cursor = Action.conn.cursor()
        # Execute all of the SQL queries        
        for query in self.sql_queries:
            cursor.execute(query)
        self.sql_queries = []
        # Log the action to the Actions table        
        if commit:            
            Action.conn.commit()

    def restore(self) -> None:
        """Restore the action, restoring the state of the referenced research objects to be the state in this Action."""        
        # Create a new action, where "redo_of" is set to self.id.
        action = Action(name = self.name, redo_of = self.id)
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

    def redo(self) -> None:
        """Execute the action, causing the current state of the referenced widgets and research objects to be the state in this Action."""        
        # Create a new action, where "redo_of" is set to self.id.        
        next_action = Action.next(id = self.id)
        if next_action is None:
            return
        Action.set_current_action(action = next_action)
        next_action.execute()

    def undo(self) -> None:
        """Undo the action, causing the current state of the referenced widgets and research objects to be the state of the previous Action."""
        # Log the action to the database.        
        prev_action = Action.previous(id = self.id)
        if prev_action is None:
            return        
        prev_action.redo()    

    def _get_rows_of_action(action_id: str, table_name: str) -> list:
        """Get the rows of a table that were created by an action."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT * FROM {table_name} WHERE action_id = '{action_id}'"
        return cursor.execute(sqlquery).fetchall()    

if __name__=="__main__":    
    action = Action(name = "Test Action")    
    action.execute()