"""The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 

import sqlite3, datetime
from sqlite3 import Row

from typing import Union, Type

class DataObject():
    """The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.
    All private attributes are prefixed with an underscore and are not included in the database.
    All public attributes are included in the database."""

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the data object. This has 3 parts, regardless of INSERT or UPDDATE or SELECT:
        1. Set the object's UUID.
        2. Select/set to default the rest of the object's attributes
        3. Insert/update the object in the database."""
        self._conn = sqlite3.connect('./SQL/database.db')
        self._conn.row_factory = Row # Return rows as dictionaries.
        # If the UUID was specified, ignore everything else and load the data object from the database.
        if len(args) > 0:
            try:
                assert self.is_uuid(args[0])
            except AssertionError:
                raise ValueError("Only UUID can be specified as a positional argument.")
            try:
                assert len(args) == 1
            except AssertionError:
                raise ValueError("UUID is the only allowed positional argument.")
            self.uuid = args[0] # The UUID of the data object.
            self._get() # The attributes of the data object.

            # Allow updating attributes in the constructor if any kwargs provided.
            for kwarg in kwargs:
                setattr(self, kwarg, kwargs[kwarg])
            if len(kwargs) > 0:
                self.update() # Update all attributes in the database.
            return
        
        # If the UUID was not specified, create a new data object.
        self.create_id()
        self._set_defaults()
        # Allow updating attributes in the constructor if any kwargs provided.
        for kwarg in kwargs:
            setattr(self, kwarg, kwargs[kwarg])
        self.insert()
        return
    
    def _set_defaults(self) -> None:
        """Set the default attributes of the data object."""
        self.name = "Untitled"
        self.description = "Description here."
        time = self._current_timestamp()
        self.created_at = time
        self.updated_at = time

    def is_uuid(self, id: str) -> bool:
        """Check if the provided ID is a UUID."""
        if not isinstance(id, str):
            return False
        
        if not id.startswith(self._uuid_prefix):
            return False

        return True
    
    def create_id(self) -> None:
        """Create the id for the data object."""
        self.uuid = "DS1" # For testing dataset creation.        
    
    def parse_id(self, id: str) -> str:
        """Parse the id for the data object.
        Returns the ID's type, abstract ID, and concrete ID."""
        raise NotImplementedError
    
    def _input_to_list(self, input: any) -> list:
        """Returns whether the object is a list."""
        if isinstance(input, list):
            return input
        return [i for i in input]
    
    def insert(self) -> None:
        """Insert one record into the database."""
        cursor = self._conn.cursor()
        keys_list = self._get_public_keys()
        keys = ', '.join(keys_list)
        values = [str(getattr(self, key)) for key in keys_list]
        values = '"' + '", "'.join(values) + '"' # Surround the values with quotes.
        sql = f"INSERT INTO {self._table_name} ({keys}) VALUES ({values})"
        try:
            cursor.execute(sql)
            self._conn.commit()
        except:
            raise ValueError("Error inserting into database.")

    def _get_public_keys(self) -> list[str]:
        """Return all public keys of the current object."""        
        keys = []
        for key in vars(self).keys():
            if not key.startswith('_'):
                keys.append(key)
        return keys
    
    #TODO: Change the cursor to return with RowFactory so I can use keys to index into it.
    def _get(self) -> None:
        """Get one record from the database."""
        if not self.is_uuid(self.uuid):
            raise ValueError("UUID must be specified.")
        
        cursor = self._conn.cursor()        
        sql = f'SELECT * FROM {self._table_name} WHERE uuid = "{self.uuid}"'
        cursor.execute(sql)
        # rows = cursor.fetchall()
        # Add the public attributes to the object.
        for row in cursor:
            for key in row.keys():
                setattr(self, key, row[key])

    def update(self) -> None:
        """Update all attributes of a record in the database."""        
        cursor = self._conn.cursor()   
        keys_list = self._get_public_keys()                        
        keys = ', '.join(keys_list)
        values = []
        for key in keys_list:
            values.append(str(getattr(self, key)))
        values = '", "'.join(values)
        values = '"' + values + '"' # Surround the values with quotes.
        sql = f"UPDATE {self._table_name} SET ({keys}) VALUES ({values}) WHERE id = {self.uuid}"
        cursor.execute(sql)
        self._conn.commit()

    def delete(self) -> None:
        """Delete a record from the database."""
        cursor = self._conn.cursor()        
        sql = f"DELETE FROM {self._table_name} WHERE id = {self.uuid}"
        cursor.execute(sql)
        self._conn.commit()

    def _to_uuids(self, values: list[Union[str, Type['DataObject']]]) -> list:
        """Convert a list of objects or UUIDs to a list of UUIDs."""
        uuids = []
        for value in values:
            if not isinstance(value, str):
                uuids.append(value.uuid)
            else:
                uuids.append(value)
        return uuids
    
    def _check_type(self, values: list[Union[str, Type['DataObject']]], types: list[Type['DataObject']]) -> None:
        """Check if the provided values are of the provided types."""
        for value in values:
            # Check type.
            if not isinstance(value, tuple(types)):
                raise ValueError(f"Value {value} is not of type {types}.")
            # Check is UUID.
            if isinstance(value, str):
                if not self.is_uuid(value):
                    raise ValueError(f"Value {value} is not a UUID.")

    def _get_all_parents(self, parent_uuid: str, child_table_name: str, parent_column: str) -> list[str]:
        """Get all parents of the child object type.
        Dataset > Subject > Visit > Trial > Phase."""
        sql = f'SELECT uuid FROM {child_table_name} WHERE {parent_column} = "{parent_uuid}"'
        cursor = self._conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    
    def _is_parent(self, id: str, parent_id: str, table_name: str, parent_column: str, child_column: str) -> bool:
        """Check if the provided parent type is the parent of the child object."""
        sql = f"SELECT {parent_column} FROM {table_name} WHERE {child_column} = {id} AND {parent_column} = {parent_id}"
        cursor = self._conn.cursor()
        cursor.execute(sql)
        return len(cursor.fetchall()) > 0
    
    def _get_all_children(self, parent_uuid: str, parent_column: str, child_table_name: str) -> list[str]:
        """Get all children of the parent object type.
        Dataset > Subject > Visit > Trial > Phase."""
        sql = f'SELECT uuid FROM {child_table_name} WHERE {parent_column} = "{parent_uuid}"'
        cursor = self._conn.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    
    def _is_child(self, id: str, child_id: str, table_name: str, parent_column: str, child_column: str) -> bool:
        """Check if the provided child type is the child of the parent object."""
        sql = f"SELECT {child_column} FROM {table_name} WHERE {parent_column} = {id} AND {child_column} = {child_id}"
        cursor = self._conn.cursor()
        cursor.execute(sql)
        return len(cursor.fetchall()) > 0
    
    def _current_timestamp(self):
        """Return the current timestamp."""
        return datetime.datetime.now()