from typing import Any, TYPE_CHECKING
import json
import pickle

if TYPE_CHECKING:
    from ResearchOS.action import Action
    from ResearchOS.DataObjects.data_object import DataObject

import networkx as nx

from ResearchOS.PipelineObjects.pipeline_object import PipelineObject
from ResearchOS.DataObjects.dataset import Dataset
from ResearchOS.variable import Variable
from ResearchOS.idcreator import IDCreator
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.action import Action
from ResearchOS.sql.sql_runner import sql_order_result

all_default_attrs = {}
all_default_attrs["conditions"] = {}

computer_specific_attr_names = []

numeric_logic_options = (">", "<", ">=", "<=", )
any_type_logic_options = ("==", '=', "!=", "in", "not in", "is", "is not", "contains", "not contains")
logic_options = numeric_logic_options + any_type_logic_options
plural_logic = ("in", "not in", "contains", "not contains")

class Subset(PipelineObject):
    """Provides rules to select a subset of data from a dataset."""
    
    prefix = "SS"

    def __init__(self, conditions: dict = all_default_attrs["conditions"], **kwargs):
        if self._initialized:
            return
        self.conditions = conditions
        super().__init__(**kwargs)

    ## conditions
    
    def validate_conditions(self, conditions: dict, action: Action, default: Any) -> None:
        """Validate the condition recursively.
        Example usage:
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
        """
        if conditions == default:
            return
        # Validate a single condition.
        if isinstance(conditions, list):
            if len(conditions) != 3:
                raise ValueError("Condition must be a list of length 3.")
            if not IDCreator(action.conn).is_ro_id(conditions[0]):
                raise ValueError("Variable ID must be a valid Variable ID.")
            if not ResearchObjectHandler.object_exists(conditions[0], action):
                raise ValueError("Variable must be pre-existing.")
            if conditions[1] not in logic_options:
                raise ValueError("Invalid logic.")
            if conditions[1] in numeric_logic_options and not isinstance(conditions[2], int):
                raise ValueError("Numeric logical symbols must have an int value.")
            try:
                a = json.dumps(conditions[2])
            except:
                raise ValueError("Value must be JSON serializable.")
            return

        # Validate the "and"/"or" keys.
        if not isinstance(conditions, dict):
            raise ValueError("Condition must be a dict.")
        if "and" not in conditions and "or" not in conditions:
            raise ValueError("Condition must contain an 'and' or 'or' key.")
        if "and" in conditions and "or" in conditions:
            raise ValueError("Condition cannot contain both 'and' and 'or' keys.")
        
        for key, value in conditions.items():
            if key not in ("and", "or"):
                raise ValueError("Invalid key in condition.")
            if not isinstance(value, list):
                raise ValueError("Value must be a list.")
            if not isinstance(value, (list, dict)):
                raise ValueError("Value must be a list of lists or dicts.")
            a = [self.validate_conditions(cond, action, default = default) for cond in value] # Assigned to a just to make interpreter happy.
            
    def get_subset(self, action: Action) -> nx.MultiDiGraph:
        """Resolve the conditions to the actual subset of data."""
        from ResearchOS.DataObjects.data_object import DataObject
        # 1. Get the dataset.
        dataset_id = self.get_dataset_id()
        ds = Dataset(id = dataset_id)
        schema_id = self.get_current_schema_id(ds.id)

        # 2. For each node_id in the address_graph, check if it meets the conditions.
        nodes_for_subgraph = []
        G = ds.get_addresses_graph()
        sorted_nodes = list(nx.topological_sort(G))
        subclasses = DataObject.__subclasses__()

        # Loop through all conditions in the conditions dict. Handle when the condition is a list or a dict.
        conditions_list = []
        numbered_dict = self.extract_and_replace_lists(self.conditions, conditions_list)
        vr_ids = [cond[0] for cond in conditions_list]

        # Get the hashes
        sqlquery_raw = "SELECT data_blob_hash, dataobject_id, vr_id FROM data_values WHERE vr_id IN ({}) AND schema_id = ?".format(", ".join(["?" for _ in vr_ids]))
        sqlquery = sql_order_result(action, sqlquery_raw, ["dataobject_id", "vr_id"], single = False, user = True, computer = False)
        params = tuple(vr_ids) + (schema_id,)
        cursor = action.conn.cursor()
        result = cursor.execute(sqlquery, params).fetchall()

        # Get the values
        conn_data = ResearchObjectHandler.pool_data.get_connection()
        cursor_data = conn_data.cursor()
        sqlquery = "SELECT data_blob, data_blob_hash FROM data_values_blob WHERE data_blob_hash IN ({})".format(", ".join(["?" for _ in result]))
        params = tuple([x[0] for x in result])
        values = cursor_data.execute(sqlquery, params).fetchall()
        ResearchObjectHandler.pool_data.return_connection(conn_data)
        values = [list(item) for item in values]

        for value in values:
            value[0] = pickle.loads(value[0])

        # Put the values into a dict.
        vr_values = {}
        for row in result:
            if row[2] not in vr_values:
                vr_values[row[2]] = {}
            if row[1] not in vr_values[row[2]]:
                vr_values[row[2]][row[1]] = None
            blob_hash_idx = [x[1] for x in values].index(row[0])
            vr_values[row[2]][row[1]] = values[blob_hash_idx][0]


        for node_id in sorted_nodes:
            # cls = [cls for cls in subclasses if cls.prefix == node_id[0:2]][0]
            # node = cls(id = node_id)
            if not self.meets_conditions(node_id, self.conditions, G, vr_values, action):
                continue
            print(node_id)
            curr_nodes = [node_id]
            curr_nodes.extend(nx.ancestors(G, node_id))
            nodes_for_subgraph.extend([node_id for node_id in curr_nodes if node_id not in nodes_for_subgraph])
        return G.subgraph(nodes_for_subgraph) # Maintains the relationships between all of the nodes in the subgraph.

    def extract_and_replace_lists(self, data, extracted_lists: list, counter=[0]):
        """ Recursively traverses the data structure, replaces each list with a unique number, and extracts the lists. """
        if isinstance(data, list):
            # Append the current list to the extracted lists
            extracted_lists.append(data)
            # Replace the list with the current counter value
            number = counter[0]
            counter[0] += 1
            return number
        elif isinstance(data, dict):
            # Traverse dictionary and process each value
            return {key: [self.extract_and_replace_lists(item, extracted_lists, counter) if isinstance(item, list) else item for item in value] if isinstance(value, list) else self.extract_and_replace_lists(value, extracted_lists, counter) for key, value in data.items()}
        else:
            # For other data types, return as is
            return data


    def meets_conditions(self, node_id: str, conditions: dict, G: nx.MultiDiGraph, vr_values: dict, action: Action) -> bool:
        """Check if the node_id meets the conditions."""
        if isinstance(conditions, dict):
            if "and" in conditions:
                for cond in conditions["and"]:
                    if not self.meets_conditions(node_id, cond, G, vr_values, action):
                        return False
                return True
                # return all([self.meets_conditions(node_id, cond, G) for cond in conditions["and"]])
            if "or" in conditions:
                return any([self.meets_conditions(node_id, cond, G, vr_values, action) for cond in conditions["or"]])
                    
        # Check the condition.
        vr_id = conditions[0]
        logic = conditions[1]
        value = conditions[2]
        try:
            vr_value = vr_values[vr_id][node_id]
            found_attr = True
        except:
            anc_nodes = nx.ancestors(G, node_id)
            found_attr = False
            for anc_node_id in anc_nodes:
                try:
                    vr_value = vr_values[vr_id][anc_node_id]
                except:
                    continue
                found_attr = True
                break
        if not found_attr:
            return False

        # This is probably shoddy logic, but it'll serve as a first pass to handle None types.
        if logic in plural_logic:
            if logic == "contains" and vr_value is None:
                return False
            elif logic == "not contains" and vr_value is None and value is not None:                
                return True
            elif logic == "in" and value is None:
                return False
            elif logic == "not in" and value is None:
                return True

        # Numeric
        bool_val = False
        if logic == ">" and vr_value > value:
            bool_val = True
        elif logic == "<" and vr_value < value:
            bool_val = True
        elif logic == ">=" and vr_value >= value:
            bool_val = True
        elif logic == "<=" and vr_value <= value:
            bool_val = True
        # Any type
        elif logic in ["==","="] and vr_value == value:
            bool_val = True
        elif logic == "!=" and vr_value != value:
            bool_val = True
        elif logic == "in" and vr_value in value:
            bool_val = True
        elif logic == "not in" and vr_value not in value:
            bool_val = True
        elif logic == "is" and vr_value is value:
            bool_val = True
        elif logic == "is not" and vr_value is not value:
            bool_val = True
        elif logic == "contains" and value in vr_value:
            bool_val = True
        elif logic == "not contains" and not value in vr_value:
            bool_val = True

        return bool_val
            
        

    

    