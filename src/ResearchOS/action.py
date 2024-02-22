import datetime, sqlite3
from datetime import timezone

from ResearchOS.db_connection_factory import DBConnectionFactory
from ResearchOS.idcreator import IDCreator
from ResearchOS.current_user import CurrentUser
from ResearchOS.sqlite_pool import SQLiteConnectionPool

class Action():
    """An action is a set of SQL queries that are executed together."""
    
    def __init__(self, name: str = None, id: str = None, redo_of: str = None, user_object_id: str = None, timestamp: datetime.datetime = None):           
        self.pool = SQLiteConnectionPool()
        self.sql_queries = []
        if not id:
            id = IDCreator().create_action_id()            
            if not timestamp:
                timestamp = datetime.datetime.now(timezone.utc)
            if not user_object_id:
                user_object_id = CurrentUser().get_current_user_id()
            sqlquery = f"INSERT INTO actions (action_id, user, name, datetime, redo_of) VALUES ('{id}', '{user_object_id}', '{name}', '{str(timestamp)}', '{redo_of}')"
            self.add_sql_query(sqlquery)
        # else:
        #     # Loading an existing action.
        #     sqlquery = f"SELECT * FROM actions WHERE action_id = '{id}'"
        #     result = cursor.execute(sqlquery).fetchall()            
        #     if result is None:
        #         raise AssertionError(f"Action {id} does not exist.")
        #     if len(result) > 1:
        #         raise AssertionError(f"Action {id} is not unique.")
        #     result = result[0]
        #     user_object_id = result[1]
        #     name = result[2]                        
        #     timestamp = result[3]            
        #     redo_of = result[4]

        self.commit = True
        self.id = id
        self.name = name
        self.timestamp = timestamp        
        self.redo_of = redo_of               
        self.user_object_id = user_object_id
        self.do_exec = True

    ###############################################################################################################################
    #################################################### end of dunder methods ####################################################
    ###############################################################################################################################             
    
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
    
    def add_sql_query(self, sqlquery: str, params: tuple = None) -> None:
        """Add a sql query to the action."""
        if params:
            self.sql_queries.append((sqlquery, params))
        else:
            self.sql_queries.append((sqlquery,))

    def execute(self) -> None:
        """Run all of the sql queries in the action."""
        pool = SQLiteConnectionPool(name = "main")        
        conn = pool.get_connection()
        cursor = conn.cursor()
        # Ensure that all of the SQL queries are tuples: (sqlquery, params)
        for idx, q in enumerate(self.sql_queries):
            if not isinstance(q, (tuple)):
                self.sql_queries[idx] = (q,)

        # Execute all of the SQL queries.
        if len(self.sql_queries) == 0:
            pool.return_connection(conn)            
            return
        for query_list in self.sql_queries:
            try:
                if len(query_list) == 1:
                    cursor.execute(query_list[0])
                else:                    
                    cursor.execute(query_list[0], query_list[1])
            except:                
                pool.return_connection(conn)
                raise ValueError(f"SQL query failed: {query_list[0]}")
        self.sql_queries = []               
        # Commit the Action.  
        if self.commit:            
            conn.commit()
        pool.return_connection(conn)

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