# Created by ChatGPT4

import sqlite3, inspect
from queue import Queue
from threading import Lock

from ResearchOS.config import Config

class SQLiteConnectionPool:
    _instance = None
    _lock = Lock()  # Ensure thread-safe singleton access

    def __new__(cls, database: str = None, pool_size: int = 5):
        config = Config()
        if database is None:
            database = config.db_file
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(SQLiteConnectionPool, cls).__new__(cls)
                cls._instance.database = database
                cls._instance.pool_size = pool_size
                cls._instance.pool = Queue(maxsize=pool_size)
                for _ in range(pool_size):
                    cls._instance.pool.put(sqlite3.connect(database))
        return cls._instance
    
    def print_caller_method_name(self):
        caller_frame = inspect.currentframe().f_back.f_back.f_back
        caller_name = caller_frame.f_code.co_name
        print("\n")
        print(f"Caller file: {caller_frame.f_code.co_filename}")
        print(f"Caller method name: {caller_name}")
        del caller_frame
        # print(inspect.stack()[1][3])

    def get_connection(self):
        self.print_caller_method_name()
        if self.pool.empty():
            raise sqlite3.Error("No available connections")
        conn = self.pool.get(block=True)
        print("Got a connection. Remaining: " + str(self.pool.qsize()))
        return conn

    def return_connection(self, connection):
        self.print_caller_method_name()
        if self.pool.full():
            connection.close()
        else:
            self.pool.put(connection)
        print("Returned a connection. Remaining: " + str(self.pool.qsize()))
        return

    def close_all(self):
        while not self.pool.empty():
            connection = self.pool.get(block=False)
            connection.close()
