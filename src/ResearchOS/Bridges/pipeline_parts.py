import weakref
from abc import abstractmethod
import json

from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action
from ResearchOS.idcreator import IDCreator

class MetaPipelineParts(type):
    def __call__(cls, *args, **kwargs):
        force_init = getattr(kwargs, "force_init", False)
        obj, found_in_cache = cls.__new__(cls, *args, **kwargs)
        if not found_in_cache or force_init:
            obj.__init__(*args, **kwargs)
        return obj

class PipelineParts(metaclass=MetaPipelineParts):

    instances = {}
    instances["Let"] = weakref.WeakValueDictionary()
    instances["Put"] = weakref.WeakValueDictionary()
    instances["Edge"] = weakref.WeakValueDictionary()
    instances["LetPut"] = weakref.WeakValueDictionary()
    instances["Dynamic"] = weakref.WeakValueDictionary()
    allowable_none_cols = []

    def __new__(cls, *args, **kwargs):
        
        id = None
        if "id" in kwargs.keys():
            id = kwargs["id"]
        cls_weakref_dict = PipelineParts.instances[cls.cls_name]
        if id in cls_weakref_dict.keys():
            return cls_weakref_dict[id], True
        instance = super().__new__(cls)
        if id is not None:
            cls_weakref_dict[id] = instance
        return instance, False
    
    def __init__(self, id: int = None, action: Action = None):
        """If id is not None, loads the object from the database. Otherwise, creates a new object in the database."""
        self.id = id

        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name = f"create_{self.cls_name}")
        self.action = action

        if not hasattr(self, "where_str"):
            self.where_str = " AND ".join([f"{col} = ?" for col in self.col_names])
        
        # Check whether the object already exists in the database.        
        prev_exists = False
        load_from_db = False
        if self.id is not None:        
            # The check against the "is_redundant_params" cache allows loading objects at all levels of the cache without hitting the database.           
            col_names_str = ", ".join(self.col_names)     
            sqlquery_raw = f"SELECT {col_names_str} FROM {self.table_name} WHERE {self.id_col} = ?"
            sqlquery = sql_order_result(action, sqlquery_raw, [self.id_col], single=False, user = True, computer = False)
            params = (self.id,)
            result = action.conn.execute(sqlquery, params).fetchall()
            if result:
                load_from_db = True
                for idx, col_name in enumerate(self.col_names):
                    value = result[0][idx] if col_name != "value" else json.loads(result[0][idx])
                    setattr(self, col_name, value)
                prev_exists = True
            else:
                # Make the params for the is_redundant_params cache.
                params = [self.id] + [action.id_num] + [getattr(self, col) if col != "value" else json.dumps(getattr(self, col)) for col in self.col_names]
                params = [p if p != [] else None for p in params] # Turn the empty list into all None values for SQL.
                if action.is_redundant_params(None, self.insert_query_name, params):
                    # If it's redundant means it should already exist but right now only does in the actions cache.                        
                    prev_exists = True
        else:
            load_from_db = True
            # 1. Search for the object in the database based on matching attributes.       
            sqlquery = f"SELECT {self.id_col} FROM {self.table_name} WHERE {self.where_str}"
            if not hasattr(self, "params"):
                params = tuple([getattr(self, col) if col != "value" else json.dumps(getattr(self, col)) for col in self.col_names])
            else:
                params = self.params
            params = tuple([p.id if hasattr(p, "id") else p for p in params])
            result = action.conn.execute(sqlquery, params).fetchall()
            if result:
                prev_exists = True
                id = result[0][0]
        if not prev_exists:
            # 2. If not found, create a new object in the database.
            idcreator = IDCreator(action.conn)
            id = idcreator.create_generic_id(self.table_name, self.id_col)
        
        self.id = id # Do this whether prev_exists or not. id guaranteed to be set at this point. 
        PipelineParts.instances[self.cls_name][self.id] = self 
        

        
        if prev_exists and not load_from_db:
            return # Nothing else to do if all of the attributes are already present?
        
        if hasattr(self, "input_args"):
            input_args = self.input_args
            params = []            
            if not input_args[0]: # There are no input_args.
                params = [tuple([id] + [action.id_num] + [None] * len(self.col_names))]
            for idx in range(len(input_args[0])):
                row = [input_args[i][idx] for i in range(len(input_args))]
                params.append(tuple([id] + [action.id_num] + row))
        else:
            input_args = [getattr(self, col) for col in self.col_names]
            start_params = [id] + [action.id_num] + [a if a != [] else None for a in input_args]
            value_idx = self.col_names.index("value") if "value" in self.col_names else None
            if value_idx:
                value_idx += 2 # The first two values are id and action_id.
                start_params[value_idx] = json.dumps(start_params[value_idx])
            params = [tuple(start_params)] # One set of params.
        for param in params:
            param = tuple([p.id if hasattr(p, "id") else p for p in param])
            if not prev_exists and not action.is_redundant_params(None, self.insert_query_name, param):
                action.add_sql_query(None, self.insert_query_name, param)
            else:
                print('a')

        if load_from_db:                                
            self.load_from_db(*input_args, action = action)

        if return_conn:
            action.commit = True
            action.execute()

    @abstractmethod
    def load_from_db(self):
        pass