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
        self.action = action

        # print(f"PipelineParts: {self.cls_name} {self.id}, Action: {action.id_num}")

        # return_conn = False
        # if action is None:
        #     return_conn = True
        #     action = Action(name = f"create_{self.cls_name}")
        # self.action = action

        # if not hasattr(self, "where_str"):
        #     self.where_str = " AND ".join([f"({col} = ? OR {col} IS NULL)" for col in self.col_names])
        
        # # Check whether the object already exists in the database.        
        # prev_exists = False
        # load_from_db = False
        # if self.id is not None:        
        #     # The check against the "is_redundant_params" cache allows loading objects at all levels of the cache without hitting the database.           
        #     col_names_str = ", ".join(self.col_names)     
        #     sqlquery_raw = f"SELECT {col_names_str} FROM {self.table_name} WHERE {self.id_col} = ?"
        #     sqlquery = sql_order_result(action, sqlquery_raw, [self.id_col], single=False, user = True, computer = False)
        #     params = (self.id,)
        #     result = action.conn.execute(sqlquery, params).fetchall()
        #     if result:
        #         load_from_db = True
        #         for idx, col_name in enumerate(self.col_names):
        #             value = result[0][idx] if col_name != "value" else json.loads(result[0][idx])
        #             setattr(self, col_name, value)
        #         prev_exists = True
        #     else:
        #         # Make the params for the is_redundant_params cache.
        #         params = [self.id] + [action.id_num] + [getattr(self, col) if col != "value" else json.dumps(getattr(self, col)) for col in self.col_names]
        #         params = [p if p != [] else None for p in params] # Turn the empty list into all None values for SQL.
        #         if action.is_redundant_params(None, self.insert_query_name, params):
        #             # If it's redundant means it should already exist but right now only does in the actions cache.                        
        #             prev_exists = True
        # else:
        #     load_from_db = True
        #     # 1. Search for the object in the database based on matching attributes.       
        #     sqlquery = f"SELECT {self.id_col} FROM {self.table_name} WHERE {self.where_str}"
        #     if not hasattr(self, "params"):
        #         params = tuple([getattr(self, col) if col != "value" else json.dumps(getattr(self, col)) for col in self.col_names])
        #     else:
        #         params = self.params
        #     params = tuple([p.id if hasattr(p, "id") else p for p in params])
        #     result = action.conn.execute(sqlquery, params).fetchall()
        #     if result:
        #         prev_exists = True
        #         id = result[0][0]
        # if not prev_exists:
        #     # 2. If not found, create a new object in the database.
        #     idcreator = IDCreator(action.conn)
        #     id = idcreator.create_generic_id(self.table_name, self.id_col)
        
        # self.id = id # Do this whether prev_exists or not. id guaranteed to be set at this point. 
        # PipelineParts.instances[self.cls_name][self.id] = self 
        
        # if prev_exists and not load_from_db:
        #     return # Nothing else to do if all of the attributes are already present?
        
        # if hasattr(self, "input_args"):
        #     input_args = self.input_args
        #     params = []            
        #     if not input_args[0]: # There are no input_args.
        #         params = [tuple([id] + [action.id_num] + [None] * len(self.col_names))]
        #     for idx in range(len(input_args[0])):
        #         row = [input_args[i][idx] for i in range(len(input_args))]
        #         params.append(tuple([id] + [action.id_num] + row))
        # else:
        #     input_args = [getattr(self, col) for col in self.col_names]
        #     start_params = [id] + [action.id_num] + [a if a != [] else None for a in input_args]
        #     value_idx = self.col_names.index("value") if "value" in self.col_names else None
        #     if value_idx:
        #         value_idx += 2 # The first two values are id and action_id.
        #         start_params[value_idx] = json.dumps(start_params[value_idx])
        #     params = [tuple(start_params)] # One set of params.
        # for param in params:
        #     param = tuple([p.id if hasattr(p, "id") else p for p in param])
        #     if not prev_exists and not action.is_redundant_params(None, self.insert_query_name, param):
        #         action.add_sql_query(None, self.insert_query_name, param)

        # if load_from_db:                                
        #     self.load_from_db(*input_args, action = action)

        # if return_conn:
        #     action.commit = True
        #     action.execute()

    @abstractmethod
    def load_from_db(self):
        pass

    def load_from_db2(self, id: int, action: Action):
        """Load the object from the database given its ID."""
        from ResearchOS.Bridges.put import Put
        sqlquery = f"SELECT {', '.join(self.col_names)} FROM {self.table_name} WHERE {self.id_col} = ?"
        params = (id,)
        result = action.conn.execute(sqlquery, params).fetchall()
        if not result:
            query_params = action.dobjs["all"][self.insert_query_name]["None"]
            ids = [param[0] for param in query_params]            
            if id not in ids:
                raise ValueError(f"Object not found in the database: {self.cls_name} {id}") 
            if not isinstance(self, Put):
                attrs = {col_name: query_params[0][idx + 2] for idx, col_name in enumerate(self.col_names)}
                self.init_from_attrs(**attrs, action = action)
            else:
                attrs = {col_name: [] for col_name in self.col_names}
                for row in query_params:
                    if row[0] != id:
                        continue
                    for idx, col_name in enumerate(self.col_names):
                        value = row[idx + 2]
                        attrs[col_name].append(value)   
                self.init_from_attrs(**attrs, action = action)
            return
        do_append = False
        if isinstance(self, Put):
            do_append = True
        for row in result:            
            for idx, col_name in enumerate(self.col_names):
                value = row[idx] if col_name != "value" else json.loads(row[idx])
                if do_append:
                    if not hasattr(self, col_name) or not isinstance(getattr(self, col_name), list):
                        setattr(self, col_name, [])
                    getattr(self, col_name).append(value)
                else:
                    setattr(self, col_name, value)

    def assign_id(self, attrs: dict, action: Action):
        """Assigns an ID to the object."""
        idcreator = IDCreator(action.conn)
        id = idcreator.create_generic_id(self.table_name, self.id_col)
        self.id = id
        attrs[self.id_col] = id

    def get_id_if_present(self, attrs: dict, action: Action):
        """Determines whether the object needs to be saved in the database."""
        if self.id is not None:
            return self.id        
        where_str = " AND ".join([f"{col} = ?" if col is not None else f"{col} IS NULL" for col in self.col_names])  
        sqlquery = f"SELECT {self.id_col} FROM {self.table_name} WHERE {where_str}"
        if not "dynamic_vr_id" in attrs:            
            params = tuple([attrs[col] if col != "value" else json.dumps(attrs[col]) for col in self.col_names])  
            result = action.conn.execute(sqlquery, params).fetchall()
            if not result:
                return None
            else:
                return result[0][0]
        else:
            for idx in range(len(attrs["dynamic_vr_id"])):                
                params = tuple([attrs[col][idx] if col != "value" else json.dumps(attrs[col][idx]) for col in self.col_names])
                result = action.conn.execute(sqlquery, params).fetchall()
                if not result:
                    return None
            return result[0][0]
    
    def save(self, attrs: dict, action: Action):
        """Saves the object in the database."""
        from ResearchOS.Bridges.put import Put
        if not self.needs_saving():
            return
        if not isinstance(self, Put):
            params = tuple([self.id] + [action.id_num] + [attrs[col] if col != "value" else json.dumps(attrs[col]) for col in self.col_names])
            if not action.is_redundant_params(None, self.insert_query_name, params):
                action.add_sql_query(None, self.insert_query_name, params)
        else:
            for idx in range(len(self.dynamic_vrs)):
                dynamic_vr = self.dynamic_vrs[idx]
                is_input = dynamic_vr.is_input
                order_num = dynamic_vr.order_num
                is_lookup = dynamic_vr.is_lookup
                params = (self.id, action.id_num, dynamic_vr.id, is_input, order_num, is_lookup)
                if not action.is_redundant_params(None, self.insert_query_name, params):
                    action.add_sql_query(None, self.insert_query_name, params)

    def needs_saving(self):
        """Checks if the object needs to be saved in the database. Returns False if not."""
        sqlquery = f"SELECT {self.id_col} FROM {self.table_name} WHERE {self.id_col} = ?"
        params = (self.id,)
        result = self.action.conn.execute(sqlquery, params).fetchall()
        return not result
            