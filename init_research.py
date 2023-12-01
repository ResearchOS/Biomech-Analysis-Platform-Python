from SQL.database_init import DBInitializer as DBInit

default_path = "/Users/mitchelltillman/Desktop/Not_Work/Code/Python_Projects/Biomech-Analysis-Platform-Python/SQL/"

def init_research(path: str = None):
    import os
    import sys

    if path is None:
        path = default_path

    # Add the database directory to the path if it's not there
    database_path = os.path.join(default_path, 'database.db')
    if database_path not in sys.path:
        sys.path.append(database_path)

    DBInit(path)