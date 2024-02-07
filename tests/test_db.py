import os, json

from ResearchOS.db_initializer import DBInitializer
from ResearchOS.db_connection import DBConnectionSQLite

sql_settings_path = os.path.abspath("src/ResearchOS/config/sql.json")

def test_db_file_exists(temp_db_file):
    """Make sure that the database file exists after the database is first created."""
    assert not os.path.isfile(temp_db_file)

    db = DBInitializer(temp_db_file)

    assert db.db_file == temp_db_file
    assert os.path.isfile(temp_db_file)

def test_tables_exist(db_init):
    """Make sure that the tables exist in the database after the database is first created."""
    with open(sql_settings_path, "r") as file:
        data = json.load(file)
    intended_tables = data["intended_tables"]
    db_init.check_tables_exist(intended_tables)

def test_dbconnection_singleton(db_connection, temp_db_file):
    """Make sure that only one instance of the DBconnectionSQLite is created."""
    db_connection2 = DBConnectionSQLite(temp_db_file)
    assert db_connection is db_connection2
    conn1 = db_connection.conn
    conn2 = db_connection2.conn
    assert conn1 is conn2

if __name__=="__main__":
    test_tables_exist(DBInitializer("test.db"))