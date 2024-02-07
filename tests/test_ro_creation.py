import pytest

import ResearchOS as ros

classes_and_ids = [
    (ros.User, "ID1"),
    (ros.Project, "ID1"),
    (ros.Analysis, "ID1"),
    (ros.Dataset, "ID1"),
    (ros.Process, "ID1"),
    (ros.Subset, "ID1"),
    (ros.Logsheet, "ID1")
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
    """Create a new System object with only the id kwarg."""
    system = cls(id = id)
    # Check the object's common attributes.
    

    # Check the contents of the SQL tables.
        
if __name__=="__main__":
    test_happy_create_new_ro_with_id_kwarg_only(classes_and_ids[0][0], classes_and_ids[0][1])    