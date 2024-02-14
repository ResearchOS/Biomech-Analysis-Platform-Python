import ResearchOS as ros

addresses = [
        ["DS1"],
        ["DS1", "SJ1"],
        ["DS1", "SJ1", "TR1"],
        ["DS1", "SJ1", "TR2"],
        ["DS1", "SJ2"],
        ["DS1", "SJ2", "TR1"],
        ["DS1", "SJ2", "TR2"]
    ]
schema = [
        [ros.Dataset, ros.Subject],
        [ros.Subject, ros.Trial]
    ]

def test_dataset_exists():
    """Make sure that the dataset exists in the database after the dataset is first created."""
    ds = ros.Dataset(id = "ID1")
    assert isinstance(ds, ros.Dataset)
    assert ds.id == "ID1"
    assert ds.prefix == "DS"

def test_dataset_schema():
    """Make sure that the schema is correct."""
    ds = ros.Dataset(id = "ID1")    
    ds.schema = schema
    del ds
    ds = ros.Dataset(id = "ID1")
    assert ds.schema == schema

def test_dataset_addresses():
    ds = ros.Dataset(id = "DS1")
    ds.schema = schema    
    ds.addresses = addresses
    del ds
    ds = ros.Dataset(id = "DS1")
    ds.addresses == addresses

def test_add_data():
    ds = ros.Dataset(id = "DS1")
    ds.schema = schema
    ds.addresses = addresses
    vr1 = ros.Variable(id = "VR1")
    ds.vr[vr1.id] = 1
    del ds
    ds = ros.Dataset(id = "DS1")
    assert ds.vr[vr1.id] == 1


if __name__ == "__main__":
    # test_dataset_exists(id = "ID1")

    # pr1 = ros.Process(id = "PR1", name = "derivative1")
    # pr2 = ros.Process(id = "PR2", name = "derivative2")

    # pr1.input_variables = [position.id, fs.id]
    # pr1.output_variables = [velocity.id]

    # pr2.input_variables = [velocity.id, fs.id]
    # pr2.output_variables = [acceleration.id]

    # pr_static = ros.Static.Process(id = "PR", name = "derivative")
    # pr_static.level = ros.Trial
    # pr_static.method = derivative.derivative

    # Alternatives:
    # 1. Reimplement everything twice
    # 2. This
    # 3. More opaque, have "pr" and have some other way of keeping track of inputs.


    sj = ros.Subject(id = "SJ1")
    vr = ros.Variable(id = "VR1", parent = sj.id)    
    sj.name = "test"

    # Get data out
    vr1, vr2 = sj.get(vr1 = vr1.id, vr2 = vr2.id)
    # Put data in
    sj.assign(height = vr)
    # Builtin
    sj.builtin1 = 1