from copy import deepcopy

import pytest
import networkx as nx

from ResearchOS.hash_dag import hash_node, get_attrs_to_hash
from ResearchOS.custom_classes import Runnable, Variable

runnable1 = Runnable('1', 'runnable1', {
        'inputs': {'a': 1}, 
        'outputs': ["output1"], 
        'subset':'subset1',
        'function': 'function1',
        'level': 'level1',
        'batch': ['batch1']
        })
variable1 = Variable('1', 'variable1', {})

def test_hash_runnable_node_no_data_object():

    dag = nx.MultiDiGraph()    
    dag.add_node('1', node=runnable1)

    hash1 = hash_node(dag, '1')
    hash2 = hash_node(dag, '1')
    assert hash1 == hash2 # Checks deterministic hashing

    # Check that changing the UUID (which is random) does not change the hash
    runnable2 = deepcopy(runnable1)
    runnable2.id = '2'
    dag.add_node('2', node=runnable2)
    hash2 = hash_node(dag, '2')
    assert hash1 == hash2

    no_data_object_attrs, data_object_attrs = get_attrs_to_hash(runnable1)
    for attr in runnable1.attrs:
        runnable3 = deepcopy(runnable1)
        runnable3.attrs[attr] = 'new_value'        
        dag.add_node('3', node=runnable3)
        hash3 = hash_node(dag, '3', data_object = None)
        if attr in no_data_object_attrs:
            assert hash1 != hash3, f"Changing {attr} did not change the hash"
        else:
            assert hash1 == hash3, f"Changing {attr} did change the hash"
        dag.remove_node('3')

def test_hash_variable_node_no_data_object():
    
    dag = nx.MultiDiGraph()    
    dag.add_node('1', node=variable1)
    hash1 = hash_node(dag, '1')
    hash2 = hash_node(dag, '1')
    assert hash1 == hash2 # Checks deterministic hashing

    # Check that changing the UUID (which is random) does not change the hash
    variable2 = deepcopy(variable1)
    variable2.id = '2'
    dag.add_node('2', node=variable2)
    hash2 = hash_node(dag, '2')
    assert hash1 == hash2


def test_hash_changing_connectivity():
    
    dag = nx.MultiDiGraph()
    dag.add_node('1', node=runnable1)
    dag.add_node('2', node=variable1)
    dag.add_edge('1', '2')
    hash1 = hash_node(dag, '2') # With the edge
    dag.remove_edge('1', '2')
    hash2 = hash_node(dag, '2') # Edge removed
    assert hash1 != hash2

if __name__ == '__main__':
    pytest.main([__file__])