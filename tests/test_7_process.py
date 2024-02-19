import pytest

from examples.derivative import derivative
import ResearchOS as ros

def test_process_exists(db_connection):
    """Make sure that the variable exists in the database after the variable is first created."""
    pr = ros.Process(id = "PR1")
    assert isinstance(pr, ros.Process)
    assert pr.id == "PR1"
    assert pr.name == pr.id
    assert pr.prefix == "PR"

def test_process_method_fails_if_not_callable(db_connection):
    """Make sure that the method is a callable function."""
    pr = ros.Process(id = "PR1")
    method = "not a callable function"
    try:
        pr.method = method
        assert False
    except ValueError:
        assert True

def test_happy_process_method_class_method(db_connection):
    """Make sure that the method is a callable function."""
    pr = ros.Process(id = "PR1")
    method = pr.load
    pr.method = method
    del pr
    pr = ros.Process(id = "PR1")
    assert pr.method == method

def test_happy_process_method_function(db_connection):
    """Make sure that the method is a callable function."""
    pr = ros.Process(id = "PR1")
    method = derivative
    pr.method = method
    del pr
    pr = ros.Process(id = "PR1")
    assert pr.method == method

def test_level_fails_if_not_type(db_connection):
    """Make sure that the level is a type."""
    pr = ros.Process(id = "PR1")
    level = "not a type"
    try:
        pr.level = level
        assert False
    except ValueError:
        assert True

def test_happy_level(db_connection):
    """Make sure that the level is a type."""
    pr = ros.Process(id = "PR1")
    level = ros.Trial
    pr.level = level
    del pr
    pr = ros.Process(id = "PR1")
    assert pr.level == level

def test_input_vr_fails_if_not_dict(db_connection):
    """Make sure that the input variables are correct."""
    pr = ros.Process(id = "PR1")
    inputs = "not a dict"
    try:
        pr.input_vr = inputs
        assert False
    except ValueError:
        assert True

def test_input_vr_fails_if_not_valid_variable_names(db_connection):
    """Make sure that the input variables are correct."""
    pr = ros.Process(id = "PR1")
    inputs = {
        "not a valid variable name": "VR1"
    }
    try:
        pr.input_vr = inputs
        assert False
    except ValueError:
        assert True

def test_input_vr_fails_if_not_valid_variable_ids(db_connection):
    """Make sure that the input variables are correct."""
    pr = ros.Process(id = "PR1")
    inputs = {
        "valid_variable_name": "not a valid variable id"
    }
    try:
        pr.input_vr = inputs
        assert False
    except ValueError:
        assert True

def test_input_vr_fails_if_vr_does_not_exist(db_connection):
    """Make sure that the input variables are correct."""
    pr = ros.Process(id = "PR1")
    inputs = {
        "valid_variable_name": "VR1"
    }
    try:
        pr.input_vr = inputs
        assert False
    except ValueError:
        assert True

def test_happy_input_vr(db_connection):
    """Make sure that the input variables are correct."""
    pr = ros.Process(id = "PR1")
    vr = ros.Variable(id = "VR1", name = "var1")
    vr2 = ros.Variable(id = "VR2", name = "var2")
    vr3 = ros.Variable(id = "VR3", name = "var3")
    inputs = {
        "valid_variable_name": vr.id
    }
    pr.method = derivative
    pr.set_input_vrs(data = vr, fs = vr2)
    pr.set_output_vrs(deriv = vr3)
    del pr
    pr = ros.Process(id = "PR1")
    assert pr.input_vr == inputs