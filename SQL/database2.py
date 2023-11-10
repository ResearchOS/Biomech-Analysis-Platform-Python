from sqlalchemy import create_engine, Column, Integer, String, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, Session

Base = declarative_base()

class Dataset(Base):
    __tablename__ = 'datasets'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    description = Column(String)
    uuid = Column(String)
    created_at = Column(String)
    updated_at = Column(String)
    subjects = relationship('Subject', back_populates='dataset')  # Parent

class Subject(Base):
    __tablename__ = 'subjects'
    id = Column(Integer, primary_key=True)
    dataset_id = Column(Integer, ForeignKey('datasets.id'))
    name = Column(String)
    uuid = Column(String)
    dataset = relationship('Dataset', back_populates='subjects')  # Child

if __name__ == "__main__":
    engine = create_engine('sqlite:///database.db', echo=True)  # Use your actual database URL
    Base.metadata.create_all(engine)
    
    session = Session(engine)

    d = Dataset(name='test')

    session.add(d)
    session.commit()

    # Optional: Query the database to verify the insertion
    queried_dataset = session.query(Dataset).filter_by(name='test').first()
    print("Queried Dataset:", queried_dataset)
