from abc import abstractmethod
import json

from ResearchOS.load_save_strategies.load_save_strategies import LoadSaveStrategy
from ResearchOS.action import Action

class DefaultStrategy(LoadSaveStrategy):
    """Class to provide a default way to load and save attributes of a research object from the database."""

    @abstractmethod
    def load(research_object, *args, **kwargs):
        """Load attributes from the database using the research_object_attributes table. These are attributes of the research object that are not specific to any other table."""
        cursor = Action.conn.cursor()
        sqlquery = f"SELECT action_id, attr_id, attr_value, target_object_id FROM research_object_attributes WHERE object_id = '{research_object.id}'"
        unordered_attr_result = cursor.execute(sqlquery).fetchall()
        ordered_attr_result = ResearchObject._get_time_ordered_result(unordered_attr_result, action_col_num = 0)
        if len(unordered_attr_result) == 0:
            raise ValueError("No object with that ID exists.")         
                             
        curr_obj_attr_ids = [row[1] for row in ordered_attr_result]
        num_attrs = len(list(set(curr_obj_attr_ids))) # Get the number of unique action ID's.
        used_attr_ids = []
        attrs = {}
        attrs["id"] = research_object.id
        for row in ordered_attr_result:            
            attr_id = row[1]
            attr_value_json = row[2]
            target_object_id = row[3]
            if attr_id in used_attr_ids:
                continue
            else:
                used_attr_ids.append(attr_id)                        

            attr_name = ResearchObject._get_attr_name(attr_id)
            # Translate the attribute from string to the proper type/format.                     
            try:
                from_json_method = eval("self.from_json_" + attr_name)
                attr_value = from_json_method(attr_value_json)
            except AttributeError as e:
                attr_value = json.loads(attr_value_json)

            try:
                method = eval(f"self.load_{attr_name}")            
                method(attr_value)
            except AttributeError as e:
                pass
            # Now that the value is loaded as the proper type/format (and is not None), validate it.
            try:
                if attr_value is not None:
                    validate_method = eval("self.validate_" + attr_name)
                    validate_method(attr_value)
            except AttributeError as e:
                pass
            attrs[attr_name] = attr_value
            if len(used_attr_ids) == num_attrs:
                break # Every attribute is accounted for.
                
        research_object.__dict__.update(attrs)
    
    @abstractmethod
    def save(self, *args, **kwargs):
        """Save attributes to the database using the research_object_attributes table. These are attributes of the research object that are not specific to any other table."""
        return NotImplementedError