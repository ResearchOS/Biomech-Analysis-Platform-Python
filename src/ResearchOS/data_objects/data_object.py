"""The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage.""" 

import sqlite3, datetime
from sqlite3 import Row

from typing import Union, Type, Any
import weakref

from research_object import ResearchObject

class DataObject(ResearchObject):
    """The abstract base class for all data objects. Data objects are the ones not in the digraph, and represent some form of data storage."""

    def __setattr__(self, __name: str, __value: Any) -> None:
        super().__setattr__(__name, __value)        
        
        # Set attributes of the object in the database.
        pass


    