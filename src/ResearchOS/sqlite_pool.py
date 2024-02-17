# Created by ChatGPT4

import sqlite3
from queue import Queue
from threading import Lock

from ResearchOS.config import Config

class SQLiteConnectionPool:
    _instance = None
    _lock = Lock()  # Ensure thread-safe singleton access

    def __new__(cls, database: str = None, pool_size: int = 10):
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

    def get_connection(self):
        if self.pool.empty():
            raise sqlite3.Error("No available connections")
        conn = self.pool.get(block=True)
        print("Connections remaining: " + str(self.pool.qsize()))
        return conn

    def return_connection(self, connection):
        if self.pool.full():
            connection.close()
        else:
            self.pool.put(connection)
        print("Connections remaining: " + str(self.pool.qsize()))

    def close_all(self):
        while not self.pool.empty():
            connection = self.pool.get(block=False)
            connection.close()
