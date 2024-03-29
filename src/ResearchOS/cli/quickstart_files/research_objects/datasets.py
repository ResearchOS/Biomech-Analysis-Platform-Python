import ResearchOS as ros

dataset = ros.Dataset(id = "DS1", name = "My Dataset")
dataset.dataset_path = "data/"
dataset.schema = [
    [ros.Dataset, MyDataObject]
]