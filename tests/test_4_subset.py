import pytest

import ResearchOS as ros

def test_subset_exists(db_connection):
    """Make sure that the subset exists in the database after the subset is first created."""
    sb = ros.Subset(id = "SS1")
    assert isinstance(sb, ros.Subset)
    assert sb.id == "SS1"
    assert sb.prefix == "SS"

def test_subset_conditions(db_connection):
    """Make sure that the conditions are correct."""
    sb = ros.Subset(id = "SS1")
    vr1 = ros.Variable(id = "VR1")
    conditions = {
        "and": [
            [vr1.id, "<", 4],
            {
                "or": [
                    [vr1.id, ">", 2],
                    [vr1.id, "=", 7]
                ]
            }
        ]
    }
    sb.conditions = conditions
    del sb
    sb = ros.Subset(id = "SS1")
    assert sb.conditions == conditions