## Recommended Folder Structure
```plaintext
project_folder/
│-- research_objects/
│   │-- data_objects/
│   |    |-- my_data_object1.py
│   |    |-- my_data_object2.py
```
Data Objects can be defined by the user in the project folder. While the exact method is flexible as long as the user subclasses ResearchOS.data_objects.DataObject, I recommend to create a `research_objects` folder in the project folder and then create a `data_objects` folder within that to define each Data Object. For each Data Object subclass, define a class that inherits from `DataObject`. For example, the following code defines a `Subject` Data Object:

```python
from ResearchOS.DataObjects.data_object import DataObject

all_default_attrs = {}
computer_specific_attr_names = []

class Subject(DataObject):

    prefix: str = "SJ" # Must be unique within the project.
    
    def __init__(self, **kwargs):
        if self._initialized:
            return
        super().__init__(**kwargs)
```
All of the above code must be present to define a custom Data Object. The class name and the two letter prefix should be changed to suit your needs. The `all_default_attrs` and `computer_specific_attr_names` variables are used to define the attributes of the Data Object and should be left empty. The `prefix` attribute is a two letter prefix that is used to generate the Data Object's ID. The `__init__` method should be left as is.