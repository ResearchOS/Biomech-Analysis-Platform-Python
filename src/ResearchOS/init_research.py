from SQL.database_init import DBInitializer as DBInit
from config import ProdConfig

def init_research(db_file: str):
    DBInit(db_file)

if __name__ == "__main__":
    init_research(ProdConfig.db_file)