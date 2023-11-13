from SQL.database import User, Dataset, Subject, Visit, Trial, Phase, Variable, Subvariable
from SQL.database_init import DBInitializer


import os
from unittest import TestCase
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

Base = declarative_base()

db_file = 'test_database.db'
engine = create_engine(f'sqlite:///{db_file}', echo=True)
Session = sessionmaker(bind = engine)

class TestDatabase(TestCase):

    db_file: str = db_file

    def setup_class(self):     
        Base.metadata.create_all(engine)
        self.session = Session()

    def teardown_class(self):
        os.remove(self.db_file)

    # def test_db_exists(self):
    #     os.path.isfile(self.db_file)

    # def test_user(self):
    #     self.assertTrue(True)

    def test_create_dataset(self):
        d = Dataset(id=1, name='test')
        assert d.id == 1
        assert d.name == 'test'
        self.session.add(d)
        self.session.commit()
        # Perform a select statement to make sure the dataset was created in SQL.
        d_db = Dataset.query.filter_by(id=1).first()
        assert d_db.id == 1
        assert d_db.name == 'test'

if __name__=="__main__":
    TestDatabase().setup_class()
    TestDatabase().test_create_dataset()
    TestDatabase().teardown_class()