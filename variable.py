"""Representation of a Variable object. Subclass of DataObject."""

from data_object import DataObject
from dataclasses import dataclass

@dataclass
class Variable(DataObject):
    id: str