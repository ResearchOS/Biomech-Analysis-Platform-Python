import datetime, sqlite3
from datetime import timezone
from typing import Union
import os

from ResearchOS.idcreator import IDCreator
from ResearchOS.current_user import CurrentUser
from ResearchOS.sqlite_pool import SQLiteConnectionPool

count = 0

# Load the SQL queries from the sql directory.
queries = {}
dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql")
file_paths = []
for file_name in os.listdir(dir):
    if not file_name.endswith(".sql"):
        continue
    file_paths.append(os.path.join(dir, file_name))
for file_path in file_paths:
    file_name, ext = os.path.splitext(os.path.basename(file_path))
    with open(file_path, "r") as f:
        queries[file_name] = f.read()

class Action():
    """An action is a set of SQL queries that are executed together."""

    latest_action_id: str = None
    queries: dict = queries
    
    def __init__(self, name: str = None, action_id: str = None, redo_of: str = None, timestamp: datetime.datetime = None, commit: bool = False, exec: bool = True, force_create: bool = False):        
        pool = SQLiteConnectionPool()
        self.conn = pool.get_connection()
        if not action_id:
            action_id = IDCreator(self.conn).create_action_id(check = False)            
        if not timestamp:
            timestamp = datetime.datetime.now(timezone.utc)

        # Set up for the queries.
        self.dobjs = {}

        self.force_create = force_create
        self.creation_params = (action_id, name, timestamp, redo_of) # The parameters for the creation query.
        self.is_created = False # Indicates that the action has not been created in the database yet.        
        self.id = action_id
        self.name = name
        self.timestamp = timestamp        
        self.redo_of = redo_of
        self.commit = commit # False by default. If True, the action will be committed to the database. Overrides self.exec.
        self.exec = exec # True to run cursor.execute() and False to skip it.

    def add_sql_query(self, dobj_id: str, query_name: str, params: tuple = None, group_name: str = "all") -> None:
        """Add a sqlquery to the action. Can be a raw SQL query (one input) or a parameterized query (two inputs).
        Parameters can be either a tuple (for one query) or a list of tuples (for multiple queries)."""
        if not params:
            return # Do I need this?
        if not isinstance(params, tuple):            
            raise ValueError(f"params must be a tuple or list, not {type(params)}.")
        if group_name not in self.dobjs:
            self.dobjs[group_name] = {}
        if query_name not in self.dobjs[group_name]:
            self.dobjs[group_name][query_name] = {} # Initialize the dobj_id if it doesn't exist.
        if dobj_id not in self.dobjs[group_name][query_name]:
            self.dobjs[group_name][query_name][dobj_id] = [] # Initialize the query_name if it doesn't exist for this dobj_id        
        
        self.dobjs[group_name][query_name][dobj_id].append(params) # Allows for multiple params to be added at once with "executemany"

    def execute(self, return_conn: bool = True) -> None:
        """Run all of the sql queries in the action."""
        if not self.exec:
            return
        global count
        count += 1
        # print(f"Action.execute() called {count} times.")
        pool = SQLiteConnectionPool(name = "main")

        any_queries = False
        if self.dobjs:
            any_queries = True

        if not any_queries and not self.force_create:
            pool.return_connection(self.conn)
            self.conn = None
            return
        
        cursor = self.conn.cursor()
        if not self.is_created or self.force_create:
            self.is_created = True
            cursor.execute(self.queries["action_insert"], self.creation_params)

        # Execute all of the SQL queries.
        for group_name in self.dobjs:
            for query_name in self.dobjs[group_name]:
                query = self.queries[query_name]
                params_list = []
                for dobj_id in self.dobjs[group_name][query_name]:
                    if len(self.dobjs[group_name][query_name][dobj_id]) == 0:
                        continue

                    for param in self.dobjs[group_name][query_name][dobj_id]:
                        if param not in params_list:
                            params_list.append(param)
                    # if self.dobjs[group_name][query_name][dobj_id] not in params_list:
                    #     params_list.extend(self.dobjs[group_name][query_name][dobj_id])
                num_params = len(params_list)
                try:
                    for i in range(0, num_params, 50):
                        if i < num_params - 50:                        
                            curr_params = params_list[i:i+50]
                        else:
                            curr_params = params_list[i:]
                        cursor.executemany(query, curr_params)
                except sqlite3.OperationalError as e:
                    cursor.rollback()
                    raise ValueError(f"SQL query failed: {query}")
        self.dobjs = {}

        # Commit the Action.  
        if self.commit:
            # print("Commit count:", count)
            self.conn.commit()
            if return_conn:
                pool.return_connection(self.conn)
                self.conn = None
    
    # @staticmethod
    # def get_latest_action(user_id: str = None) -> "Action":
    #     """Get the most recent action performed chronologically for the current user."""
    #     conn = DBConnectionFactory.create_db_connection().conn
    #     if not user_id:
    #         user_id = CurrentUser(conn).get_current_user_id()
    #     sqlquery = f"SELECT action_id FROM actions WHERE user_id = '{user_id}' ORDER BY timestamp DESC LIMIT 1"
    #     try:
    #         result = conn.cursor().execute(sqlquery).fetchone()
    #     except sqlite3.OperationalError:
    #         raise AssertionError(f"User {user_id} does not exist.")
    #     if result is None:
    #         return None
    #     id = result[0]
    #     return Action(id = id)
    
    # ###############################################################################################################################
    # #################################################### end of abstract methods ##################################################
    # ###############################################################################################################################

    # def next(self) -> "Action":
    #     """Get the next action after this action."""
    #     cursor = self.conn.cursor()
    #     id = self.id
    #     timestamp = self.timestamp
    #     sqlquery = f"SELECT action_id FROM actions WHERE timestamp <= '{str(timestamp)}' ORDER BY timestamp DESC LIMIT 1"
    #     result = cursor.execute(sqlquery).fetchone()
    #     if result is None:
    #         return None
    #     id = result[0]
    #     return Action(id = id)

    # def previous(self) -> "Action":
    #     """Get the previous action before this action."""
    #     cursor = self.conn.cursor()
    #     id = self.id
    #     timestamp = self.timestamp
    #     sqlquery = f"SELECT action_id FROM actions WHERE timestamp >= '{str(timestamp)}' ORDER BY timestamp ASC LIMIT 1"
    #     result = cursor.execute(sqlquery).fetchone()
    #     if result is None:
    #         return None
    #     id = result[0]
    #     return Action(id = id)
    
    

    # def restore(self) -> None:
    #     """Restore the action, restoring the state of the referenced research objects to be the state in this Action."""        
    #     # Create a new action, where "redo_of" is set to self.id.
    #     action = Action(name = self.name, redo_of = self.id)
    #     cursor = self.conn.cursor()
    #     table_names = ["data_values", "research_attributes", "data_address_schemas", "data_addresses"]
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