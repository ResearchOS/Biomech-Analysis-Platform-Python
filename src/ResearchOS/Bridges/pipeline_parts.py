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
        self.id = id

        return_conn = False
        if action is None:
            return_conn = True
            action = Action(name = f"create_{self.cls_name}")
        self.action = action

        if not hasattr(self, "where_str"):
            self.where_str = " AND ".join([f"{col} = ?" for col in self.col_names])
        
        if self.id is not None:       
            col_names_str = ", ".join(self.col_names)     
            sqlquery_raw = f"SELECT {col_names_str} FROM {self.table_name} WHERE {self.id_col} = ?"
            sqlquery = sql_order_result(action, sqlquery_raw, [self.id_col], single=False, user = True, computer = False)
            params = (self.id,)
            result = action.conn.execute(sqlquery, params).fetchall()
            if not result:
                raise ValueError(f"{self.cls_name} with id {self.id} not found in database.")
            if hasattr(self, "input_args"):
                input_args = self.input_args
            else:
                input_args = [r for r in result[0]]
        else:
            # 1. Search for the object in the database based on matching attributes.       
            sqlquery = f"SELECT {self.id_col} FROM {self.table_name} WHERE {self.where_str}"
            if not hasattr(self, "params"):
                params = tuple([getattr(self, col) if col != "value" else json.dumps(getattr(self, col)) for col in self.col_names])
            else:
                params = self.params
            result = action.conn.execute(sqlquery, params).fetchall()
            if result:
                id = result[0][0]
                create_new = False
            else:
                # 2. If not found, create a new object in the database.
                create_new = True
                idcreator = IDCreator(action.conn)
                id = idcreator.create_generic_id(self.table_name, self.id_col)
            self.id = id            
            if hasattr(self, "input_args"):
                input_args = self.input_args
                params = []
                if not input_args[0]:
                    params = [tuple([id] + [action.id_num] + [None] * len(self.col_names))]
                for idx in range(len(input_args[0])):
                    row = [input_args[i][idx] for i in range(len(input_args))]
                    params.append(tuple([id] + [action.id_num] + row))
            else:
                input_args = []
                for col in self.col_names:
                    tmp = getattr(self, col)
                    try:
                        col_val = None if (tmp is None or len(tmp) == 0) else tmp
                    except:
                        col_val = tmp # scalars don't have a len() method.
                    if col == "value":
                        col_val = json.dumps(col_val)
                    input_args.append(col_val)
                params = [tuple([id] + [action.id_num] + input_args)] # One set of params.
            if create_new:
                for param in params:
                    action.add_sql_query(None, self.insert_query_name, param)
                
        # Saves and loads object to/from the database.
        self.load_from_db(*input_args, action = action)
        # 4. Store the object in the cache.  
        PipelineParts.instances[self.cls_name][self.id] = self            

        if return_conn:
            action.commit = True
            action.execute()

    @abstractmethod
    def load_from_db(self):
        pass