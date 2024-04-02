import ResearchOS as ros
from paths import DATASET_PATH

class MyDataObject(ros.DataObject):

    prefix: str = "two_character_prefix"

class MyDataObject2(ros.DataObject):

    prefix: str = "different_two_character_prefix"

# Define the dataset object.
dataset = ros.Dataset(id = "DS1")
dataset.schema = []
dataset.dataset_path = DATASET_PATH