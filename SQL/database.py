from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Table, UniqueConstraint, Engine
from sqlalchemy.orm import relationship, declarative_base, Session, registry

Base = declarative_base()

# Define the association tables for many-to-many relationships
phase_data = Table('phase_data', Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('file_path', String),
    Column('scalar_data', String),
    Column('trial_id', Integer, ForeignKey('trials.id')),
    Column('phase_id', Integer, ForeignKey('phases.id')),
    Column('var_id', Integer, ForeignKey('variables.id')),
    Column('subvar_id', Integer, ForeignKey('subvariables.id')),
    UniqueConstraint('trial_id', 'phase_id', 'var_id', 'subvar_id', name='phase_data_unique_constraint')
)

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    email = Column(String)
    password = Column(String)

class Dataset(Base):
    __tablename__ = 'datasets'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    uuid = Column(String)
    created_at = Column(String)
    updated_at = Column(String)
    subjects = relationship('Subject', back_populates='dataset') # Parent

class Subject(Base):
    __tablename__ = 'subjects'
    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey('datasets.id'))
    name = Column(String)
    uuid = Column(String)
    dataset = relationship('Dataset', back_populates='subjects') # Child
    visits = relationship('Visit', back_populates='subject') # Parent

class Visit(Base):
    __tablename__ = 'visits'
    id = Column(Integer, primary_key=True)
    subject_id = Column(Integer, ForeignKey('subjects.id'))
    name = Column(String)
    uuid = Column(String)
    subject = relationship('Subject', back_populates='visits') # Child
    trials = relationship('Trial', back_populates='visit') # Parent

class Trial(Base):
    __tablename__ = 'trials'
    id = Column(Integer, primary_key=True)
    visit_id = Column(Integer, ForeignKey('visits.id'))
    name = Column(String)
    uuid = Column(String)
    visit = relationship('Visit', back_populates='trials') # Child
    phases = relationship('Phase', secondary = phase_data, back_populates='trials') # Parent

class Phase(Base):
    __tablename__ = 'phases'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    uuid = Column(String)
    trials = relationship('Trial', secondary = phase_data, back_populates='phases', viewonly = True) # Child
    variables = relationship('Variable', secondary = phase_data, back_populates='phases', viewonly=True) # Parent

class Variable(Base):
    __tablename__ = 'variables'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    uuid = Column(String)
    phases = relationship('Phase', secondary = phase_data, back_populates='variables', viewonly=True) # Child
    subvariables = relationship('Subvariable', secondary = phase_data, back_populates='variables', viewonly=True) # Parent

class Subvariable(Base):
    __tablename__ = 'subvariables'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    uuid = Column(String)
    variables = relationship('Variable', secondary = phase_data, back_populates='subvariables', viewonly=True) # Child

def db_init(db_file: str = 'test_database.db') -> Engine:
    engine = create_engine(f'sqlite:///{db_file}', echo=True)
    Base.metadata.create_all(engine)
    return engine

if __name__=="__main__":
    db_init()