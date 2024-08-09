
import pytest

from ResearchOS.input_classifier import classify_input_type, load_constant_from_file
from ResearchOS.custom_classes import Constant, DataFilePath, DataObjectName, InputVariable, LoadConstantFromFile, LogsheetVariable, Unspecified

def test_classify_input_type():
    # Test case 1: Unspecified input
    input1 = '?'
    expected_class1, expected_attrs1 = Unspecified, {}
    assert classify_input_type(input1) == (expected_class1, expected_attrs1)

    # Test case 2: DataObjectName input
    input2 = "__data_object_name__"
    expected_class2, expected_attrs2 = DataObjectName, {}
    assert classify_input_type(input2) == (expected_class2, expected_attrs2)

    # Test case 3: LogsheetVariable input
    input3 = "__logsheet__"
    expected_class3, expected_attrs3 = LogsheetVariable, {}
    assert classify_input_type(input3) == (expected_class3, expected_attrs3)

    # Test case 4: InputVariable input
    input4 = "smoothData.mocapData"
    expected_class4, expected_attrs4 = InputVariable, {}
    assert classify_input_type(input4) == (expected_class4, expected_attrs4)

    # Test case 5: Constant input (string)
    input5 = "constant_value"
    expected_class5, expected_attrs5 = Constant, {'value': input5}
    assert classify_input_type(input5) == (expected_class5, expected_attrs5)

    # Test case 6: Constant input (dictionary)
    input6 = {'key': 'value'}
    expected_class6, expected_attrs6 = Constant, {'value': input6}
    assert classify_input_type(input6) == (expected_class6, expected_attrs6)

    # Test case 7: LoadConstantFromFile input (TOML file)
    toml_path = 'tests/fixtures/constants.toml'
    input7 = {'__load_file__': toml_path}
    expected_class7, expected_attrs7 = LoadConstantFromFile, {'value': load_constant_from_file(toml_path)}
    assert classify_input_type(input7) == (expected_class7, expected_attrs7)

    # Test case 8: LoadConstantFromFile input (JSON file)
    json_path = 'tests/fixtures/constants.json'
    input8 = {'__load_file__': json_path}
    expected_class8, expected_attrs8 = LoadConstantFromFile, {'value': load_constant_from_file(json_path)}
    assert classify_input_type(input8) == (expected_class8, expected_attrs8)

    # Test case 9: DataFilePath input
    input9 = {'__data_file__': {"ext": ".c3d", "levels": ["Subject", "Trial"]}}
    expected_class9, expected_attrs9 = DataFilePath, {'value': {"ext": ".c3d", "levels": ["Subject", "Trial"]}}
    assert classify_input_type(input9) == (expected_class9, expected_attrs9)

    # Test case 10: Constant input (other types)
    input10 = 123
    expected_class10, expected_attrs10 = Constant, {'value': input10}
    assert classify_input_type(input10) == (expected_class10, expected_attrs10)

if __name__ == "__main__":
    pytest.main([__file__])