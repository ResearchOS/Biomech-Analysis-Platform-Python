# Created by ChatGPT4

import sqlite3, inspect
from queue import Queue, Empty
from threading import Lock

from ResearchOS.config import Config

class SQLiteConnectionPool:
    _instances = {"main": None, "data": None}
    _lock = Lock()  # Ensure thread-safe singleton access

    def __new__(cls, name: str = "main", pool_size: int = 10):
        config = Config()        
        if name == "main":
            database = config.db_file
        elif name == "data":
            database = config.data_db_file
        with cls._lock:
            if cls._instances[name] is None:
                cls._instances[name] = super(SQLiteConnectionPool, cls).__new__(cls)
                cls._instances[name].database = database
                cls._instances[name].pool_size = pool_size
                cls._instances[name].pool = Queue(maxsize=pool_size)                
                for _ in range(pool_size):
                    cls._instances[name].pool.put(sqlite3.connect(database))
                cls._instances[name].checked_out_connections = set()
        return cls._instances[name]
    
    def print_caller_method_name(self):
        caller_frame = inspect.currentframe().f_back.f_back.f_back
        caller_name = caller_frame.f_code.co_name
        print("\n")
        print(f"Caller file: {caller_frame.f_code.co_filename}")
        print(f"Caller method name: {caller_name}")
        del caller_frame
        # print(inspect.stack()[1][3])

    def get_connection(self):
        # self.print_caller_method_name()
        if self.pool.empty():
            raise sqlite3.Error("No available connections")
        conn = self.pool.get(block=True)
        self.checked_out_connections.add(conn)
        print("Got a connection. Remaining: " + str(self.pool.qsize()))
        return conn

    def return_connection(self, connection):
        # self.print_caller_method_name()
        # Ensure that the transaction is over. 
        # If I am returning the connection, there's nothing else for it to do anyways.
        connection.rollback()
        if self.pool.full():
            connection.close()
        else:
            self.pool.put(connection)
        self.checked_out_connections.discard(connection)
        print("Returned a connection. Remaining: " + str(self.pool.qsize()))
        return

    def close_all(self):
        while not self.pool.empty():
            connection = self.pool.get(block=False)
            connection.close()

    # def commit_and_return_all(self):
    #     with self._lock:
    #         # Commit transactions for idle connections in the pool
    #         all_connections = []
    #         while not self.pool.empty():
    #             try:
    #                 conn = self.pool.get_nowait()
    #                 conn.commit()
    #                 all_connections.append(conn)
    #             except Empty:
    #                 break

    #         # Commit transactions for checked-out connections
    #         for conn in self.checked_out_connections:
    #             conn.commit()

    #         # Return all connections back to the pool
    #         for conn in self.checked_out_connections:
    #             self.return_connection(conn)

    #         # Return idle connections back to the pool
    #         # for conn in idle_connections:
    #         #     self.pool.put(conn)

            
