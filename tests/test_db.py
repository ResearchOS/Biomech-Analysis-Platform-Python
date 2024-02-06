import os, json

import pytest

from ResearchOS.database_init import DBInitializer

sql_settings_path = os.path.abspath("src/ResearchOS/config/sql.json")

def test_db_file_exists(temp_db_file):
    assert not os.path.isfile(temp_db_file)

    db = DBInitializer(temp_db_file)

    assert db.db_file == temp_db_file
    assert os.path.isfile(temp_db_file)

def test_tables_exist(db):
    with open(sql_settings_path, "r") as file:
        data = json.load(file)
    intended_tables = data["intended_tables"]
    db.check_tables_exist(intended_tables)

if __name__=="__main__":
    test_tables_exist(DBInitializer("test.db"))