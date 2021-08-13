from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base
engine = create_engine('sqlite:///bank.db', echo = True)

Base = declarative_base()

class Transaction(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    branchid = Column( Integer, nullable=False)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)

class Customer(Base):
    __tablename__ = 'customer'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    custid = Column(Integer, primary_key=True)
    createdate = Column(Integer)
    fname = Column(String(250), nullable=False)
    lname = Column(String(250), nullable=False)

Base.metadata.create_all(engine)