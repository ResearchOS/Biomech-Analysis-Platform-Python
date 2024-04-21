import weakref
from abc import abstractmethod

from ResearchOS.sql.sql_runner import sql_order_result
from ResearchOS.action import Action
from ResearchOS.idcreator import IDCreator

class PipelineParts():

    instances = {}
    instances.Let = weakref.WeakValueDictionary()
    instances.Put = weakref.WeakValueDictionary()
    instances.Edge = weakref.WeakValueDictionary()
    instances.LetPut = weakref.WeakValueDictionary()    

    def __new__(cls, *args, **kwargs):
        
        id = None
        if "id" in kwargs.keys():
            id = kwargs["id"]
        cls_weakref_dict = getattr(PipelineParts.instances, cls.__name__)
        if id in cls_weakref_dict.keys():
            return cls_weakref_dict[id]
        instance = super().__new__(cls)
        if id is not None:
            cls_weakref_dict[id] = instance
        return instance
    
    def __init__(self, id: int = None, action: Action = None):
        if hasattr(self, "id") and self.id is not None:
            return # Loaded from cache.
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
            input_args = result
        else:
            # 1. Search for the object in the database based on matching attributes.            
            sqlquery_raw = f"SELECT {self.id_col} FROM {self.table_name} WHERE {self.where_str}"
            sqlquery = sql_order_result(action, sqlquery_raw, self.col_names, single=False, user = True, computer = False)
            if not hasattr(self, "params"):
                params = tuple([getattr(self, col) for col in self.col_names])
            else:
                params = self.params
            result = action.conn.execute(sqlquery, params).fetchall()
            if result:
                id = result[0][0]
            else:
                # 2. If not found, create a new object in the database.
                idcreator = IDCreator(action.conn)
                id = idcreator.create_generic_id(self.table_name, self.id_col)
            params = []
            for col in self.col_names:
                params.append(getattr(self, col))
            params = tuple([id] + [action.action_id_num] + params)
            if not hasattr(self, "params"):
                input_args = list(params)
            else:
                input_args = self.params
            if not self.id:
                action.add_sql_query(None, self.insert_query_name, params)
                
        # Saves and loads object to/from the database.
        self.load_from_db(*input_args)
        # 4. Store the object in the cache.  
        PipelineParts.instances[self.cls_name][self.id] = self            

        if return_conn:
            action.commit = True
            action.execute()

    @abstractmethod
    def load_from_db(self, result):
        pass