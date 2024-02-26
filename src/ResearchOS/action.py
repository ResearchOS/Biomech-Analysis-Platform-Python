import datetime, sqlite3
from datetime import timezone
from typing import Union

from ResearchOS.idcreator import IDCreator
from ResearchOS.current_user import CurrentUser
from ResearchOS.sqlite_pool import SQLiteConnectionPool

count = 0

class Action():
    """An action is a set of SQL queries that are executed together."""

    latest_action_id: str = None
    
    def __init__(self, name: str = None, id: str = None, redo_of: str = None, user_object_id: str = None, timestamp: datetime.datetime = None, commit: bool = False):
        pool = SQLiteConnectionPool()
        self.conn = pool.get_connection()
        self.sql_queries = []
        if not id:
            id = IDCreator(self.conn).create_action_id(check = False)            
            if not timestamp:
                timestamp = datetime.datetime.now(timezone.utc)
            if not user_object_id:
                user_object_id = CurrentUser(self).get_current_user_id()

        self.creation_query = "INSERT INTO actions (action_id, user, name, datetime, redo_of) VALUES (?, ?, ?, ?, ?)"
        self.is_created = False # Indicates that the action has not been created in the database yet.
        self.commit = commit # False by default.
        self.id = id
        self.name = name
        self.timestamp = timestamp        
        self.redo_of = redo_of               
        self.user_object_id = user_object_id
        self.do_exec = True

    def add_sql_query(self, sqlquery: str, params: Union[tuple, list] = None) -> None:
        """Add a sqlquery to the action. Can be a raw SQL query (one input) or a parameterized query (two inputs).
        Parameters can be either a tuple (for one query) or a list of tuples (for multiple queries)."""
        if params:
            if isinstance(params, list):
                any_wrong = any([not isinstance(p, tuple) for p in params])
                if any_wrong:
                    raise ValueError(f"params must be a list of tuples, not {type(p)}.")            
            elif isinstance(params, tuple):
                params = list(params)
            else:
                raise ValueError(f"params must be a tuple or list, not {type(params)}.")                            
            self.sql_queries.append((sqlquery, params)) # Allows for multiple params to be added at once with "executemany"
        else:
            self.sql_queries.append((sqlquery,))

    def execute(self, return_conn: bool = True) -> None:
        """Run all of the sql queries in the action."""
        global count
        count += 1
        print(f"Action.execute() called {count} times.")
        any_queries = len(self.sql_queries) > 0
        pool = SQLiteConnectionPool(name = "main")

        if not any_queries:
            if return_conn:
                pool.return_connection(self.conn)
                self.conn = None
            return        
        
        cursor = self.conn.cursor()
        if not self.is_created:
            self.is_created = True
            self.sql_queries.insert(0, (self.creation_query, [(self.id, self.user_object_id, self.name, self.timestamp, self.redo_of)]))

        # Execute all of the SQL queries.
        for query_list in self.sql_queries:
            try:
                if len(query_list) == 1: # If there are no parameters.
                    cursor.execute(query_list[0])
                else: # If there are parameters.
                    params = query_list[1]
                    num_params = len(params)

                    # I read somewhere that 50 is a good batch size.
                    for i in range(0, num_params, 50):
                        if i < num_params - 50:                        
                            curr_params = params[i:i+50]
                        else:
                            curr_params = params[i:]
                    cursor.execute(query_list[0], curr_params) # Execute 1-50 queries at a time.
            except sqlite3.OperationalError as e:
                raise ValueError(f"SQL query failed: {query_list[0]}")
        self.sql_queries = []

        # Commit the Action.  
        if self.commit and any_queries:            
            self.conn.commit()

        if return_conn:
            pool.return_connection(self.conn)
            self.conn = None
    
    @staticmethod
    def get_latest_action(user_id: str = None) -> "Action":
        """Get the most recent action performed chronologically for the current user."""
        conn = DBConnectionFactory.create_db_connection().conn
        if not user_id:
            user_id = CurrentUser(conn).get_current_user_id()
        sqlquery = f"SELECT action_id FROM actions WHERE user_object_id = '{user_id}' ORDER BY timestamp DESC LIMIT 1"
        try:
            result = conn.cursor().execute(sqlquery).fetchone()
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
        cursor = self.conn.cursor()
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
        cursor = self.conn.cursor()
        id = self.id
        timestamp = self.timestamp
        sqlquery = f"SELECT action_id FROM actions WHERE timestamp >= '{str(timestamp)}' ORDER BY timestamp ASC LIMIT 1"
        result = cursor.execute(sqlquery).fetchone()
        if result is None:
            return None
        id = result[0]
        return Action(id = id)
    
    

    # def restore(self) -> None:
    #     """Restore the action, restoring the state of the referenced research objects to be the state in this Action."""        
    #     # Create a new action, where "redo_of" is set to self.id.
    #     action = Action(name = self.name, redo_of = self.id)
    #     cursor = self.conn.cursor()
    #     table_names = ["data_values", "research_object_attributes", "data_address_schemas", "data_addresses"]
    #     column_labels_list = [["action_id", "address_id", "schema_id", "VR_id", "PR_id", "scalar_value"], 
    #                      ["action_id", "object_id", "attr_id", "attr_value", "child_of"],
    #                      ["schema_id", "level_name", "level_index", "action_id", "dataset_id"],
    #                      ["address_id", "schema_id", "level0", "level1", "level2", "level3", "level4", "level5", "level6", "level7", "level8", "level9", "action_id"]]
    #     for idx, table_name in enumerate(table_names):
    #         labels = column_labels_list[idx]
    #         rows = Action._get_rows_of_action(action_id = self.id, table_name = table_name)
    #         labels_formatted = ", ".join(labels)
    #         action_id_index = labels.index("action_id")
    #         for row in rows:                
    #             row[action_id_index] = action.id # Update the action_id to the new action.
    #             row_formatted = ", ".join(row)
    #             sqlquery = f"INSERT INTO {table_name} ({labels_formatted}) VALUES ({row_formatted})"
    #             cursor.execute(sqlquery, row)
        
    #     self.conn.commit()

    # def redo(self) -> None:
    #     """Execute the action, causing the current state of the referenced widgets and research objects to be the state in this Action."""        
    #     # Create a new action, where "redo_of" is set to self.id.        
    #     next_action = Action.next(id = self.id)
    #     if next_action is None:
    #         return
    #     Action.set_current_action(action = next_action)
    #     next_action.execute()

    # def undo(self) -> None:
    #     """Undo the action, causing the current state of the referenced widgets and research objects to be the state of the previous Action."""
    #     # Log the action to the database.        
    #     prev_action = Action.previous(id = self.id)
    #     if prev_action is None:
    #         return        
    #     prev_action.redo()    

    # def _get_rows_of_action(action_id: str, table_name: str) -> list:
    #     """Get the rows of a table that were created by an action."""
    #     cursor = self.conn.cursor()
    #     sqlquery = f"SELECT * FROM {table_name} WHERE action_id = '{action_id}'"
    #     return cursor.execute(sqlquery).fetchall()    

if __name__=="__main__":    
    action = Action(name = "Test Action")    
    action.execute()