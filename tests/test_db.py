# import sys
# sys.path.append('/Users/mitchelltillman/Desktop/Not_Work/Code/Python_Projects/Biomech-Analysis-Platform-Python')

from SQL.database import db_init, User, Dataset, Subject, Visit, Trial, Phase, Variable, Subvariable
from SQL.database_init import DBInitializer


import os
from unittest import TestCase
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

Base = declarative_base()

db_file = 'test_database.db'
engine = db_init(db_file = db_file)

class TestDatabase(TestCase):

    db_file: str = db_file
    
    def setup_class(self):     
        self.session = Session(engine)
        

    def teardown_class(self):
        os.remove(self.db_file)

    # def test_db_exists(self):
    #     os.path.isfile(self.db_file)

    # def test_user(self):
    #     self.assertTrue(True)

    def test_create_dataset(self):
        d = Dataset(id=1, name='test', description='test', uuid='ABC123', created_at='Jan 1, 1970', updated_at='Jan 1, 1970')
        assert d.id == 1
        assert d.name == 'test'
        self.session.add(d)
        self.session.commit()
        # Perform a select statement to make sure the dataset was created in SQL.
        d_db = self.session.query(Dataset).filter_by(name='test').first()
        assert d_db.id == 1
        assert d_db.name == 'test'
        assert d_db.description == 'test'
        assert d_db.uuid == 'ABC123'
        assert d_db.created_at == 'Jan 1, 1970'
        assert d_db.updated_at == 'Jan 1, 1970'

if __name__=="__main__":
    test_db = TestDatabase()
    test_db.setup_class()
    test_db.test_create_dataset()
    test_db.teardown_class()