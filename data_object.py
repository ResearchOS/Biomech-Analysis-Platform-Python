"""The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 

import sqlite3
from typing import Protocol

class DataObject(Protocol):
    """The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage."""

    conn: sqlite3.Connection

    def __init__(self, conn: sqlite3.Connection) -> None:
        """Initialize the data object."""
        self.conn = conn

    def get_info(self) -> dict:
        """Returns the information of the data object."""
        raise NotImplementedError
    
    def input_to_list(self, input: any) -> list:
        """Returns whether the object is a list."""
        if isinstance(input, list):
            return input
        return [i for i in input]
    
    def insert(self, table_name: str, data: dict) -> None:
        """Insert one record into the database."""
        cursor = self.conn.cursor()
        columns = ', '.join(data.keys())
        placeholders = ', '.join('?' * len(data))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, list(data.values()))
    
    #TODO: Change the cursor to return with RowFactory so I can use keys to index into it.
    def get(self, table_name: str, id: list[int]) -> any:
        """Get a set of records from the database."""
        id = self.input_to_list(id)
        cursor = self.conn.cursor()
        ids_str = ", ".join(id)
        sql = f"SELECT * FROM {table_name} WHERE id IN ({ids_str})"
        cursor.execute(sql)
        return cursor.fetchall()

    def update(self, table_name: str, id: int, data: list[dict]) -> None:
        """Update a set of records in the database."""
        id_list = self.input_to_list(id)
        data_list = self.input_to_list(data)
        cursor = self.conn.cursor()
        id_str = ", ".join(id_list)
        for id in id_str:
            for data in data_list:
                columns = ', '.join(data.keys())
                placeholders = ', '.join('?' * len(data))
                sql = f"UPDATE {table_name} SET ({columns}) VALUES ({placeholders}) WHERE id = {id}"
                cursor.execute(sql, list(data.values()))

    def delete(self, table_name: str, id: int) -> None:
        """Delete a set of records from the database."""
        id_list = self.input_to_list(id)
        cursor = self.conn.cursor()
        id_str = ", ".join(id_list)
        sql = f"DELETE FROM {table_name} WHERE id IN ({id_str})"
        cursor.execute(sql)

    def get_all_parents(self, id: str, table_name: str, parent_column: str, child_column: str) -> list[str]:
        """Get all parents of the child object type.
        Dataset > Subject > Visit > Trial > Phase."""
        sql = f"SELECT {parent_column} FROM {table_name} WHERE {child_column} = {id}"
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    
    def is_parent(self, id: str, parent_id: str, table_name: str, parent_column: str, child_column: str) -> bool:
        """Check if the provided parent type is the parent of the child object."""
        sql = f"SELECT {parent_column} FROM {table_name} WHERE {child_column} = {id} AND {parent_column} = {parent_id}"
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return len(cursor.fetchall()) > 0
    
    def get_all_children(self, id: str, table_name: str, parent_column: str, child_column: str) -> list[str]:
        """Get all children of the parent object type.
        Dataset > Subject > Visit > Trial > Phase."""
        sql = f"SELECT {child_column} FROM {table_name} WHERE {parent_column} = {id}"
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    
    def is_child(self, id: str, child_id: str, table_name: str, parent_column: str, child_column: str) -> bool:
        """Check if the provided child type is the child of the parent object."""
        sql = f"SELECT {child_column} FROM {table_name} WHERE {parent_column} = {id} AND {child_column} = {child_id}"
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return len(cursor.fetchall()) > 0