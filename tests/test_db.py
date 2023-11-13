from SQL.database import db_init, User, Dataset, Subject, Visit, Trial, Phase, Variable, Subvariable, phase_data
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
        pass
        # os.remove(self.db_file)

    def test_db_exists(self):
        os.path.isfile(self.db_file)

    # def test_user(self):
    #     self.assertTrue(True)

    def test_create_dataset(self):
        d = Dataset(id=1, name='test', description='test', uuid='ABC123', created_at='Jan 1, 1970', updated_at='Jan 1, 1970')
        self.session.add(d)
        self.session.commit()
        # Perform a select statement to make sure the data was created in SQL.
        d_db = self.session.query(Dataset).filter_by(name='test').first()
        assert d_db.id == 1
        assert d_db.name == 'test'
        assert d_db.description == 'test'
        assert d_db.uuid == 'ABC123'
        assert d_db.created_at == 'Jan 1, 1970'
        assert d_db.updated_at == 'Jan 1, 1970'

    def test_create_subject(self):
        s = Subject(id=1, dataset_id=1, name='test', uuid='ABC123')
        self.session.add(s)
        self.session.commit()
        # Perform a select statement to make sure the data was created in SQL.
        s_db = self.session.query(Subject).filter_by(name='test').first()
        assert s_db.id == 1
        assert s_db.dataset_id == 1
        assert s_db.name == 'test'
        assert s_db.uuid == 'ABC123'

    def test_create_visit(self):
        v = Visit(id=1, subject_id=1, name='test', uuid='ABC123')
        self.session.add(v)
        self.session.commit()
        # Perform a select statement to make sure the data was created in SQL.
        v_db = self.session.query(Visit).filter_by(name='test').first()
        assert v_db.id == 1
        assert v_db.subject_id == 1
        assert v_db.name == 'test'
        assert v_db.uuid == 'ABC123'

    def test_create_trial(self):
        t = Trial(id=1, visit_id=1, name='test', uuid='ABC123')
        self.session.add(t)
        self.session.commit()
        # Perform a select statement to make sure the data was created in SQL.
        t_db = self.session.query(Trial).filter_by(name='test').first()
        assert t_db.id == 1
        assert t_db.visit_id == 1
        assert t_db.name == 'test'
        assert t_db.uuid == 'ABC123'

    def test_create_phase(self):
        p = Phase(id=1, name='test', uuid='ABC123')
        self.session.add(p)
        self.session.commit()
        # Perform a select statement to make sure the data was created in SQL.
        p_db = self.session.query(Phase).filter_by(name='test').first()
        assert p_db.id == 1
        assert p_db.name == 'test'
        assert p_db.uuid == 'ABC123'

    def test_create_variable(self):
        v = Variable(id=1, name='test', uuid='ABC123')
        self.session.add(v)
        self.session.commit()
        # Perform a select statement to make sure the data was created in SQL.
        v_db = self.session.query(Variable).filter_by(name='test').first()
        assert v_db.id == 1
        assert v_db.name == 'test'
        assert v_db.uuid == 'ABC123'

    def test_create_subvariable(self):
        sv = Subvariable(id=1, name='test', uuid='ABC123')
        self.session.add(sv)
        self.session.commit()
        # Perform a select statement to make sure the data was created in SQL.
        sv_db = self.session.query(Subvariable).filter_by(name='test').first()
        assert sv_db.id == 1
        assert sv_db.name == 'test'
        assert sv_db.uuid == 'ABC123'

    def test_create_phase_date(self):
        t = Trial(id=1, visit_id=1, name='test', uuid='ABC123')
        p = Phase(id=1, name='test', uuid='ABC123')
        v = Variable(id=1, name='test', uuid='ABC123')
        sv = Subvariable(id=1, name='test', uuid='ABC123')
        t.phases.append(p)
        p.variables.append(v)
        v.subvariables.append(sv)
        self.session.commit()
        # Perform a select statement to make sure the data was created in SQL.
        row = self.session.query(phase_data).filter_by(trial_id=1, phase_id=1, var_id=1, subvar_id=1).first()
        assert row.trial_id == 1
        assert row.phase_id == 1


if __name__=="__main__":
    test_db = TestDatabase()
    test_db.setup_class()
    test_db.test_create_phase_date()
    test_db.teardown_class()