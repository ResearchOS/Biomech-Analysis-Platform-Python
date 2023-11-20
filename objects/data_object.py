"""The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 

import sqlite3, datetime
from sqlite3 import Row

from typing import Union, Type
import weakref

db_file_test: str = 'tests/test_database.db'
db_file_production: str = 'database.db'

abstract_id_len = 6
instance_id_len = 3

class DataObject():
    """The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.
    All private attributes are prefixed with an underscore and are not included in the database.
    All public attributes are included in the database."""

    _db_file: str = db_file_test
    _instances = weakref.WeakValueDictionary()
    _conn = sqlite3.connect(_db_file)
    _conn.row_factory = Row # Return rows as dictionaries.

    def __new__(cls, *args, **kwargs):
        """Create a new data object. If the object already exists, return the existing object."""
        uuid, id = _get_uuid_and_id(cls._table_name, cls._id_prefix, args, kwargs)
        if uuid in cls._instances:
            return cls._instances[uuid]
        else:
            instance = super(DataObject, cls).__new__(cls)
            cls._instances[uuid] = instance
            instance.uuid = uuid
            instance.id = id
            return instance

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the data object, whether loading or creating one.
        Case 1. UUID specified as a positional argument. No other arguments allowed.
        Case 2. ID specified as a positional argument (get the latest version)
        Case 3. UUID specified as a keyword argument.
        Case 4. ID specified as a keyword argument.
        Case 5. No args or UUID/ID kwargs provided.
        Case 6. ID specified as a positional or keyword argument and UUID specified as a keyword argument (specific version of object).

        Bad cases:
        Case 1. ID in args and kwargs
        Case 2. UUID in args and kwargs
        Case 3. More than one positional argument.        

        1. Set the object's UUID and ID.
        2. Select/set to default the rest of the object's attributes
        3. Insert the object in the database."""      
        
        in_db = self._get() # The attributes of the data object. Also return Boolean for whether we are loading or creating the object.

        if 'uuid' in kwargs:
            del kwargs['uuid']

        # If the object is not in the database, set the defaults and insert it.
        if not in_db:
            self._set_defaults()
            # If no kwargs provided, insert the object. Check for kwargs is so it doesn't get double inserted after update() if kwargs provided.
            if len(kwargs) == 0:                
                self.insert()

        # Allow updating attributes here in the constructor if any kwargs provided.
        for kwarg in kwargs:
            setattr(self, kwarg, kwargs[kwarg])
        if len(kwargs) > 0:
            self.update()

    def _get_uuid_and_id(self, args: tuple, kwargs: dict) -> None:
        """Get the UUID and ID from the arguments."""
        uuid, id = _get_uuid_and_id(self._table_name, self._id_prefix, args, kwargs)
        self.uuid = uuid
        self.id = id
    
    def _set_defaults(self) -> None:
        """Set the default attributes of the data object."""
        self.name = "Untitled"
        self.description = "Description here."
        time = self._current_timestamp()
        self.timestamp = time

    def is_uuid(self, uuid: str) -> bool:
        """Check if the provided ID is a UUID."""        
        if uuid is None:
            uuid = self.id
        return _is_uuid(uuid)
    
    def create_uuid(self) -> None:
        """Create the id for the data object."""
        self.uuid = _create_uuid()

    def is_id(self, id: str) -> bool:
        """Check if the provided ID is a valid ID."""
        if id is None:
            self.id = id
        raise NotImplementedError

    def create_id(self) -> None:
        """Create the id for the data object."""
        self.id = _create_id(self._id_prefix, self._table_name)
        prefix, abstract, instance = self.parse_id(self.id)        
        if not prefix == self._id_prefix:
            raise ValueError(f"ID prefix {prefix} does not match object's prefix: {self._id_prefix}.")
        self.abstract_id = abstract
        self.instance_id = instance

    def parse_id(self, id: str) -> str:
        """Parse the id for the data object.
        Returns the ID's type, abstract ID, and concrete ID."""
        prefix = self.id.split("_")[0][:2]
        abstract_id = self.id.split("_")[0][2:]
        instance_id = self.id.split("_")[1]
        return prefix, abstract_id, instance_id
    
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
        except sqlite3.IntegrityError as e:
            if e.sqlite_errorcode == 1555:
                return
            elif e.sqlite_errorcode == 1299:
                self.missing_parent_error()
                return

    def _get_public_keys(self) -> list[str]:
        """Return all public keys of the current object."""        
        keys = []
        for key in vars(self).keys():
            if not key.startswith('_'):
                keys.append(key)
        return keys
    
    #TODO: Change the cursor to return with RowFactory so I can use keys to index into it.
    def _get(self) -> bool:
        """Get one record from the database."""
        if not self.is_uuid(self.uuid):
            raise ValueError("UUID must be specified in the proper format.")
        
        cursor = self._conn.cursor()        
        sql = f'SELECT * FROM {self._table_name} WHERE uuid = "{self.uuid}"'
        cursor.execute(sql)
        # rows = cursor.fetchall()
        # Add the public attributes to the object.
        in_db = False
        for row in cursor:
            in_db = True
            for key in row.keys():
                if key in ["created_at", "updated_at"]:
                    setattr(self, key, datetime.datetime.strptime(row[key], '%Y-%m-%d %H:%M:%S.%f'))
                else:
                    setattr(self, key, row[key])
        return in_db

    def update(self) -> None:
        """Update all attributes of a record in the database.
        Actually performs an insert with new UUID (same ID), following Type 2 slowly changing dimension practice."""        
        self.insert()

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
        values = self._input_to_list(values)
        if values is None:
            return
        for value in values:
            # Check type.
            if not isinstance(value, tuple(types)):
                raise ValueError(f"Value {value} is not of type {types}.")
            # Check is UUID.
            if isinstance(value, str):
                if not self.is_uuid(value, types[1]._uuid_prefix):
                    raise ValueError(f"Value {value} is not a UUID.")

    def _get_all_parents(self, parent_uuid: str, child_table_name: str, parent_column: str) -> list[str]:
        """Get all parents of the child object type.
        Dataset > Subject > Visit > Trial > Phase."""
        sql = f'SELECT uuid FROM {child_table_name} WHERE {parent_column} = "{parent_uuid}"'
        cursor = self._conn.cursor()
        cursor.execute(sql)
        data = []
        for row in cursor:
            data.append(row['uuid'])
        return data
    
    def _get_parent(self, child_uuid: str, child_table_name: str, parent_column: str) -> str:
        """Get the parent of the child object type.
        Dataset > Subject > Visit > Trial > Phase."""
        sql = f'SELECT {parent_column} FROM {child_table_name} WHERE uuid = "{child_uuid}"'
        cursor = self._conn.cursor()
        cursor.execute(sql)
        data = []
        for row in cursor:
            data.append(row[parent_column])
        if len(data) > 1:
            raise ValueError(f"Expected <=1 parent, got {len(data)}.")
        if len(data) == 1:
            data = data[0]
        else:
            data = None
        return data
    
    def _is_parent(self, id: str, parent_id: str, table_name: str, parent_column: str, child_column: str) -> bool:
        """Check if the provided parent type is the parent of the child object."""
        sql = f"SELECT {parent_column} FROM {table_name} WHERE {child_column} = {id} AND {parent_column} = {parent_id}"
        cursor = self._conn.cursor()
        cursor.execute(sql)
        return len(cursor.fetchall()) > 0
    
    def _get_all_children(self, uuid: str, column: str, child_table_name: str) -> list[str]:
        """Get all children of the parent object type.
        Dataset > Subject > Visit > Trial > Phase."""
        sql = f'SELECT uuid FROM {child_table_name} WHERE {column} = "{uuid}"'
        cursor = self._conn.cursor()
        cursor.execute(sql)
        data = []
        for row in cursor:
            data.append(row['uuid'])
        return data
    
    def _is_child(self, id: str, child_id: str, table_name: str, parent_column: str, child_column: str) -> bool:
        """Check if the provided child type is the child of the parent object."""
        sql = f"SELECT {child_column} FROM {table_name} WHERE {parent_column} = {id} AND {child_column} = {child_id}"
        cursor = self._conn.cursor()
        cursor.execute(sql)
        return len(cursor.fetchall()) > 0
    
    def _current_timestamp(self):
        """Return the current timestamp."""
        return datetime.datetime.now()
    
def _create_id(table_name: str, prefix: str, abstract: str = None, instance: str = None) -> str:
    """Create the id for the data object."""
    import random
    is_unique = False
    while not is_unique:
        if not abstract:
            abstract_new = str(hex(random.randrange(0, 16**abstract_id_len))[2:]).upper()
            abstract_new = "0" * (abstract_id_len-len(abstract_new)) + abstract_new
        else:
            abstract_new = abstract

        if not instance:
            instance_new = str(hex(random.randrange(0, 16**instance_id_len))[2:]).upper()
            instance_new = "0" * (instance_id_len-len(instance_new)) + instance_new
        else:
            instance_new = instance

        id = prefix + abstract_new + "_" + instance_new
        cursor = DataObject._conn.cursor()
        sql = f'SELECT id FROM {table_name} WHERE id = "{id}"'
        cursor.execute(sql)
        rows = cursor.fetchall()
        if len(rows) == 0:
            is_unique = True
    return id

def _create_uuid(table_name: str) -> str:
    """Create the uuid for the data object."""
    import uuid
    is_unique = False
    while not is_unique:
        uuid_out = str(uuid.uuid4()) # For testing dataset creation.
        cursor = DataObject._conn.cursor()
        sql = f'SELECT uuid FROM {table_name} WHERE uuid = "{uuid_out}"'
        cursor.execute(sql)
        rows = cursor.fetchall()
        if len(rows) == 0:
            is_unique = True
    return uuid_out

def _is_uuid(uuid: str) -> bool:
    """Check if the provided ID is a UUID."""
    import re
    uuid_pattern = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$', re.IGNORECASE)
    return bool(uuid_pattern.match(str(uuid)))

def _is_id(id: str) -> bool:
    """Check if the provided ID is a valid ID."""
    return True
    
def _get_uuid_and_id(table_name: str, prefix: str, args: tuple, kwargs: dict) -> tuple:
        """Get the UUID and ID from the arguments."""
        uuid = None
        id = None

        if len(args) > 1:
            raise ValueError("Only one positional argument allowed.")
        if len(args) == 1:
            if _is_uuid(args[0]):
                uuid = args[0]
            elif _is_id(args[0]):
                id = args[0]
            else:
                raise ValueError("Positional argument must be a UUID or ID.")
        if 'uuid' in kwargs:
            uuid = kwargs['uuid']
        if 'id' in kwargs:
            id = kwargs['id']
        if not uuid and not id:
            uuid = _create_uuid(table_name)
            id = _create_id(table_name, prefix)
        elif id:
            sql = f'SELECT uuid FROM {table_name} WHERE id = "{id}"'
            cursor = DataObject._conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            try:
                uuid = rows[-1]['uuid']
            except:
                uuid = _create_uuid(table_name)
        elif uuid:
            sql = f'SELECT id FROM {table_name} WHERE uuid = "{uuid}"'
            cursor = DataObject._conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            if len(rows) > 1:
                raise ValueError(f"Expected <=1 row, got {len(rows)}.")
            try:
                id = rows[0]['id']
            except:
                id = _create_id(table_name, prefix) # If the object doesn't exist, create it.

        return uuid, id