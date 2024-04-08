import ResearchOS as ros

class MyDataObject(ros.DataObject):

    prefix: str = "two_character_prefix"

class MyDataObject2(ros.DataObject):

    prefix: str = "different_two_character_prefix"