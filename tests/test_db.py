from SQL.database import User, Dataset, Subject, Visit, Trial, Phase, Variable, Subvariable, PhaseData
from SQL.database_init import DBInitializer


import os
from unittest import TestCase
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, declarative_base

Base = declarative_base()

class TestDatabase(TestCase):

    db_file: str = 'test_database.db'

    def setup_class(self):        
        engine = create_engine(f'sqlite:///{self.db_file}', echo=True)
        Base.metadata.create_all(engine)
        session = Session(engine)
        d = Dataset(id=1, name='test')
        session.add(d)
        session.commit()

    def teardown_class(self):
        os.delete(self.db_file)

    def test_db_exists(self):
        os.path.isfile(self.db_file)

    def test_user(self):
        self.assertTrue(True)

    def test_create_dataset(self):
        d = Dataset(id=1, name='test')
        assert d.id == 1
        assert d.name == 'test'
        # Perform a select statement to make sure the dataset was created in SQL.
        d_db = Dataset.query.filter_by(id=1).first()
        d_db.id == 1
        d_db.name == 'test'