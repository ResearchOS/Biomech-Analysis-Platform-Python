import os, json

from ResearchOS.db_initializer import DBInitializer
from ResearchOS.db_connection import DBConnectionSQLite

sql_settings_path = os.path.abspath("src/ResearchOS/config/sql.json")

def test_1_db_file_exists(temp_db_file_session):
    """Make sure that the database file exists after the database is first created.
    DBInitializer constructor also checks that the tables exist in the database. This somehow avoids errors."""
    assert not os.path.isfile(temp_db_file_session)

    db = DBInitializer(temp_db_file_session)

    assert db.db_file == temp_db_file_session
    assert os.path.isfile(temp_db_file_session)

def test_2_dbconnection_singleton(db_connection_session, temp_db_file_session):
    """Make sure that only one instance of the DBconnectionSQLite can be created."""
    db_connection2 = DBConnectionSQLite(temp_db_file_session)
    assert db_connection_session is db_connection2
    conn1 = db_connection_session.conn
    conn2 = db_connection2.conn
    assert conn1 is conn2

if __name__=="__main__":
    pass