from typing import Any

import ResearchOS as ros
from ResearchOS.action import Action
from ResearchOS.research_object_handler import ResearchObjectHandler
from ResearchOS.idcreator import IDCreator
from ResearchOS.db_connection_factory import DBConnectionFactory

all_default_attrs = {}
all_default_attrs["conditions"] = {}

complex_attrs_list = []

numeric_logic_options = (">", "<", ">=", "<=", )
any_type_logic_options = ("==", "!=", "in", "not in", "is", "is not", "contains", "not contains")
logic_options = numeric_logic_options + any_type_logic_options

class Subset(ros.PipelineObject):
    """Provides rules to select a subset of data from a dataset."""
    
    prefix = "SS"

    def __init__(self, **kwargs):
        """Initialize the attributes that are required by ResearchOS.
        Other attributes can be added & modified later."""
        super().__init__(all_default_attrs, **kwargs)

    def __setattr__(self, name: str, value: Any, action: Action = None, validate: bool = True) -> None:
        """Set the attribute value. If the attribute value is not valid, an error is thrown."""
        if name == "vr":
            raise ValueError("The attribute 'vr' is not allowed to be set for Pipeline Objects.")
        else:
            ResearchObjectHandler._setattr_type_specific(self, name, value, action, validate, complex_attrs_list)

    def load(self) -> None:
        """Load the dataset-specific attributes from the database in an attribute-specific way."""
        ros.PipelineObject.load(self) # Load the attributes specific to it being a PipelineObject.
    
    def validate_conditions(self, conditions: dict) -> None:
        """Validate the condition recursively.
        Example usage:
        condition = {
            "and": [
                {"VR": "VR000000_000", "logic": "<", "value": 4},
                {"or": [
                    {"VR": "VR000000_000", "logic": ">", "value": 2},
                    {"VR": "VR000000_000", "logic": "=", "value": 7}
                ]}
            ]
        }
        """
        # Check format for "and"/"or" keys.
        if isinstance(conditions, dict):
            if not ("and" in conditions or "or" in conditions):
                validate_single_condition(conditions)
            if "and" in conditions and "or" in conditions:
                raise ValueError("Condition cannot contain both 'and' and 'or' keys.")
            if "and" in conditions:
                if not isinstance(conditions["and"], list) or not all(isinstance(cond, dict) for cond in conditions["and"]):
                    raise ValueError("Condition must be a list of dicts.")
                return all(self.validate_conditions(cond) for cond in conditions["and"])                    
            if "or" in conditions:
                if not isinstance(conditions["or"], list) or not all(isinstance(cond, dict) for cond in conditions["or"]):
                    raise ValueError("Condition must be a list of dicts.")
                return all(self.validate_conditions(cond) for cond in conditions["or"])
        elif isinstance(conditions, list):
            return all(self.validate_conditions(cond) for cond in conditions)
        else:
            raise ValueError("Condition must be a dict or a list.")

        def validate_single_condition(condition: dict) -> None:
            """Validate a single condition, inside of an and/or list."""
            if not isinstance(condition, dict):
                raise ValueError("Condition must be a dict.")
            if "VR" not in condition:
                raise ValueError("Condition must contain a 'VR' key.")
            if not self.is_id(condition["VR"]):
                raise ValueError("Variable ID must be a valid Variable ID.")
            if "logic" not in condition.keys():            
                raise ValueError("Condition must contain a 'symbol' key.")
            if condition["logic"] not in logic_options:
                raise ValueError("Invalid logic.")
            if "value" not in condition.keys():
                raise ValueError("Condition must contain a 'value' key.")
            if condition["logic"] in numeric_logic_options and not isinstance(condition["value"], int):
                raise ValueError("Numeric logical symbols must have an int value.")
            try:
                a = json.dumps(condition["value"])
            except:
                raise ValueError("Value must be JSON serializable.")
            
    def get_subset(self) -> dict[dict]:
        """Resolve the conditions to the actual subset of data. Returns a dict of lists of dicts [of lists...].
        Example usage:
        subset = {
            "DS000000_000": {
                "SJ000000_000": {
                    "VS000000_000": {
                        "PH000000_000": {},
                         "PH000000_001": {}
                    }
                },
                "SJ000000_001": {
                    "VS000000_000": {
                        "PH000000_000": {},
                         "PH000000_001": {}
                    }
                }
            }
        }"""
        ds = ros.Dataset.get_current()
        schema_id = ds.get_schema_id()
            

    #################### Start class-specific attributes ###################
    # def add_criteria(self, var_id: str, value, logic: str) -> None:
    #     """Add a criterion to the subset.
    #     Possible values for logic are: ">", "<", ">=", "<=", "==", "!=", "in", "not in", "is", "is not", "contains", "not contains"."""
    #     from ResearchOS import Variable
    #     logic_options = [">", "<", ">=", "<=", "==", "!=", "in", "not in", "is", "is not", "contains", "not contains"]
    #     if logic not in logic_options:
    #         raise ValueError("Invalid logic value.")
    #     if var_id not in Variable.get_all_ids():
    #         raise ValueError("Invalid variable ID.")
    #     self.criteria.append((var_id, value, logic))

    #################### Start Source objects ####################
    def get_processes(self) -> list:
        """Return a list of process objects that belong to this subset."""
        from ResearchOS import Process
        pr_ids = self._get_all_source_object_ids(cls = Process)
        return [Process(id = pr_id) for pr_id in pr_ids]
    
    def get_plots(self) -> list:
        """Return a list of plot objects that belong to this subset."""
        from ResearchOS import Plot
        pl_ids = self._get_all_source_object_ids(cls = Plot)
        return [Plot(id = pl_id) for pl_id in pl_ids]
    
    def get_trials(self) -> list:
        """Return a list of trial objects that belong to this subset."""
        from ResearchOS import Trial
        tr_ids = self._get_all_source_object_ids(cls = Trial)
        return [Trial(id = tr_id) for tr_id in tr_ids]
    
    # def get_logsheets(self) -> list:
    #     """Return a list of logsheet objects that belong to this subset."""
    #     from ResearchOS import Logsheet
    #     lg_ids = self._get_all_source_object_ids(cls = Logsheet)
    #     return [Logsheet(id = lg_id) for lg_id in lg_ids]
    
    #################### Start Target objects ####################
    # TODO: Does anything go here? Do subsets point to anything?