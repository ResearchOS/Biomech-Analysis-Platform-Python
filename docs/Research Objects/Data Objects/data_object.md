# Data Objects

Data Objects are anything that can have a data value associated with it. They are analagous to factors in a statistical analysis. This definition is purposely vague as Data Objects are intended to be entirely user-defined, customizable, and extensible. This is the mechanism by which ResearchOS can be used to manage and analyze any type of data across any domain. For example, as a biomechanist my datasets consist of multiple Subjects who each have multiple Trials. Subjects and Trials are both Data Objects in this example. Another biomechanist may structure their dataset differently, with Subjects, Tasks, and Trials. Another scientist in a different domain, for example artificial intelligence, may have a dataset consisting of multiple Experiments which each have multiple Agents interacting with each other. In this case, Experiments and Agents are Data Objects. The point is that Data Objects are entirely user-defined and can be anything that can have a data value associated with it. The relationships between the Data Objects within one Dataset are defined by the Dataset's `schema` attribute as an edge list.

Data Objects can be defined by the user in the project folder. While the exact method is flexible as long as the user subclasses ResearchOS.data_objects.DataObject, I recommend to create a `research_objects` folder in the project folder and then create a `data_objects` folder within that to define each Data Object. For each Data Object subclass, define a class that inherits from `DataObject`. For example, the following code defines a `Subject` Data Object:

```python
from ResearchOS.DataObjects.data_object import DataObject

all_default_attrs = {}
computer_specific_attr_names = []

class Subject(DataObject):
    name = 'Subject'
    
    def __init__(self, **kwargs):
        if self._initialized:
            return
        super().__init__(**kwargs)
```

::: src.ResearchOS.DataObjects.data_object.DataObject