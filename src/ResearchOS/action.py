import datetime, sqlite3

from ResearchOS.db_connection_factory import DBConnectionFactory
from ResearchOS.idcreator import IDCreator
from ResearchOS.current_user import CurrentUser

class Action():
    """An action is a set of SQL queries that are executed together."""

    conn: sqlite3.Connection = DBConnectionFactory.create_db_connection().conn
    
    def __init__(self, name: str = None, id: str = None, redo_of: str = None, user_object_id: str = None, timestamp: datetime.datetime = None):                           
        cursor = Action.conn.cursor()
        if not id:
            id = IDCreator(Action.conn).create_action_id()            
            if not timestamp:
                timestamp = datetime.datetime.now(datetime.UTC)
            if not user_object_id:
                user_object_id = CurrentUser(Action.conn).get_current_user_object_id()
            # Do not commit here! When the action is executed then the action and the other db changes will be committed.
            cursor.execute("INSERT INTO actions (action_id, user_object_id, name, timestamp, redo_of) VALUES (?, ?, ?, ?, ?)", (id, user_object_id, name, timestamp, redo_of))
        else:
            # Loading an existing action.
            sqlquery = f"SELECT * FROM actions WHERE action_id = '{id}'"
            result = cursor.execute(sqlquery).fetchall()            
            if result is None:
                raise AssertionError(f"Action {id} does not exist.")
            if len(result) > 1:
                raise AssertionError(f"Action {id} is not unique.")
            result = result[0]
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
    
    def get_latest_action(user_id: str = None) -> "Action":
        """Get the most recent action performed chronologically for the current user."""
        cursor = Action.conn.cursor()
        if not user_id:
            user_id = CurrentUser(Action.conn).get_current_user_object_id()
        sqlquery = f"SELECT action_id FROM actions WHERE user_object_id = '{user_id}' ORDER BY timestamp DESC LIMIT 1"
        try:
            result = cursor.execute(sqlquery).fetchone()
        except sqlite3.OperationalError:
            raise AssertionError(f"User {user_id} does not exist.")
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