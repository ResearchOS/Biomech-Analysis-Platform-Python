"""Performs all database operations."""

import sqlite3

from database_init import DBInitializer
from data_object import DataObject

class Database(DataObject):
    def __init__(self) -> None:
        """Initialize the database."""
        db_init = DBInitializer()
        self.conn = db_init.conn

    def insert(self, data: dict) -> None:
        return super().insert(data)

    def get(self, dataset_id: int) -> dict:
        pass