import ResearchOS as ros

def test_dataset_exists():
    """Make sure that the dataset exists in the database after the dataset is first created."""
    ds = ros.Dataset(id = "ID1")
    assert isinstance(ds, ros.Dataset)
    assert ds.id == "ID1"
    assert ds.prefix == "DS"

def test_dataset_schema():
    """Make sure that the schema is correct."""
    ds = ros.Dataset(id = "ID1")
    ds.schema = [
        [ros.Dataset, ros.Subject],
        [ros.Subject, ros.Trial]
    ]
    del ds
    ds = ros.Dataset(id = "ID1")
    assert ds.schema == [
        [ros.Dataset, ros.Subject],
        [ros.Subject, ros.Trial]
    ]

if __name__ == "__main__":
    test_dataset_exists(id = "ID1")