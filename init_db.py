from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import Float
engine = create_engine('sqlite:///bank.db', echo = True)

Base = declarative_base()

class Transaction(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    branchid = Column( Integer, nullable=False)
    custid = Column(Integer, ForeignKey('customer.custid'))
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
    balance = Column(Float, nullable=False)


class Limit(Base):

    # LimitConsumer should keep track of the customer ids that have current balances greater 
    # or equal to the limit supplied to the constructor. The intro suggests -5000 for eaxmple
    __tablename__ = 'healthy-ish balances'
    id = Column(Integer, primary_key= True)
    custid = Column(Integer, ForeignKey('customer.custid'))
    createdate = Column(Integer)
    xaction = Column(String)
    balance = Column(Float)


Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)