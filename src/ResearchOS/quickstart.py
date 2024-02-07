"""Contains all of the code for the quickstart."""

from ResearchOS.db_initializer import DBInitializer as DBInit

def init_research(db_file: str):
    DBInit(db_file)

if __name__ == "__main__":
    init_research(ProdConfig.db_file)