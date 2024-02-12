import pytest

import ResearchOS as ros

classes_and_ids = [
    (ros.Project, "PJ1"),
    (ros.Analysis, "AN1"),
    (ros.Process, "PR1"),
    (ros.Subset, "SS1"),
    (ros.Logsheet, "LG1"),
    (ros.Dataset, "DS1"),
    (ros.Phase, "PH1"),
    (ros.Trial, "TR1"),
    (ros.Visit, "VT1"),
    (ros.Subject, "SJ1"),
    (ros.Variable, "VR1")    
]

@pytest.mark.parametrize("cls,id", classes_and_ids)
def test_create_new_ro_with_position_args(cls, id):
    """No positional arguments are allowed. Throws an error if any are provided."""
    try:
        system = cls(id)
    except TypeError as e:
        assert str(e) == "ResearchObject.__new__() takes 1 positional argument but 2 were given"

@pytest.mark.parametrize("cls,id", classes_and_ids)
def test_create_new_ro_with_no_args(cls, id):
    """Required to have at least one kwarg. Throws an error if none are provided."""
    try:
        system = cls()
    except ValueError as e:
        assert str(e) == "id is required as a kwarg"

@pytest.mark.parametrize("cls,id", classes_and_ids)
def test_create_new_ro_with_other_kwargs_not_id(cls, id):
    """Required to have id as a kwarg. Throws an error if not provided."""
    try:
        system = cls(other_kwarg = id)
    except ValueError as e:
        assert str(e) == "id is required as a kwarg"

@pytest.mark.parametrize("cls,id", classes_and_ids)
def test_happy_create_new_ro_with_id_kwarg_only(cls, id):
    """Create a new Research Object with only the id kwarg."""
    ro = cls(id = id)
    # Check the object's common attributes.
    assert ro.id == id
    if isinstance(ro, ros.DataObject):
        assert hasattr(ro, "vr")

    # Check the contents of the SQL tables.

@pytest.mark.parametrize("cls,id", classes_and_ids)
def test_happy_create_new_ro_with_id_kwarg_and_custom_kwargs(cls, id):
    """Create a new Research Object with the id kwarg and other kwargs."""
    ro = cls(id = id, other_kwarg = "other_kwarg")
    # Check the object's common attributes.
    assert ro.id == id
    assert ro.other_kwarg == "other_kwarg"
    assert ro.prefix == cls.prefix
    if isinstance(ro, ros.DataObject):
        assert hasattr(ro, "vr")

    # Check the contents of the SQL tables.

project_builtin_args = [
    (ros.Project, "ID1", "AN1", "DS1")
]

@pytest.mark.parametrize("cls,id,current_analysis_id,current_dataset_id", project_builtin_args)
def test_create_new_ro_with_id_kwarg_and_other_builtin_kwargs(cls, id, current_analysis_id, current_dataset_id):
    """Create a new Research Object with the id kwarg and other builtin kwargs."""
    ro = cls(id = id, other_kwarg = "other_kwarg", current_analysis_id = current_analysis_id, current_dataset_id = current_dataset_id)
    # Check the object's common attributes.
    assert ro.id == id
    assert ro.current_analysis_id == current_analysis_id
    assert ro.current_dataset_id == current_dataset_id

    # Check the contents of the SQL tables.
        
if __name__=="__main__":
    # test_create_new_ro_with_id_kwarg_and_other_builtin_kwargs(classes_and_ids[0][0], classes_and_ids[0][1])  
    test_create_new_ro_with_id_kwarg_and_other_builtin_kwargs(project_builtin_args[0][0], project_builtin_args[0][1], project_builtin_args[0][2], project_builtin_args[0][3])