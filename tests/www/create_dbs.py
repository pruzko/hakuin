import os
import random

from sqlalchemy import create_engine, Column, UnicodeText, Integer, Text, LargeBinary, Float
from sqlalchemy.orm import declarative_base, sessionmaker



random.seed(42)

DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_DBS = os.path.abspath(os.path.join(DIR_FILE, '..', 'dbs'))

DB_URIS = {
    # comment out DBMS that are not installed
    'sqlite': f'sqlite:///{os.path.join(DIR_DBS, "test_db.sqlite")}',
    'mysql': 'mysql+pymysql://hakuin:hakuin@localhost/hakuindb',
    'mssql': 'mssql+pyodbc://hakuin:hakuin@localhost/hakuindb?driver=ODBC+Driver+17+for+SQL+Server',
    'oracledb': 'oracle+cx_oracle://hakuin:hakuin@localhost/?service_name=freepdb1',
    'psql': 'postgresql+psycopg2://hakuin:hakuin@localhost/hakuindb',
}
DBS_ENG = {key: create_engine(uri) for key, uri in DB_URIS.items()}
DBS = {key: sessionmaker(bind=eng)() for key, eng, in DBS_ENG.items()}

Base = declarative_base()



class TestDataTypes(Base):
    __tablename__ = 'test_data_types'

    id = Column(Integer, primary_key=True)
    test_integers = Column(Integer)
    test_floats = Column(Float)
    test_blobs = Column(LargeBinary)
    test_texts = Column(Text)
    test_nullable = Column(Integer)



class TestUnicode(Base):
    __tablename__ = 'Ħ€ȽȽ©'

    id = Column(Integer, primary_key=True)
    test_texts = Column(UnicodeText, name='ŴǑȒȽƉ')



class TestIntOptimizations(Base):
    __tablename__ = 'test_int_optimizations'

    id = Column(Integer, primary_key=True)
    norm_dist = Column(Integer)



def clear_tables():
    for eng in DBS_ENG.values():
        Base.metadata.drop_all(eng)
        Base.metadata.create_all(eng)


def create_data_types_table(db):
    db.query(TestDataTypes).delete()
    db.add(TestDataTypes(
        id=1,
        test_integers=1,
        test_floats=1.1,
        test_blobs=bytes.fromhex('deadbeef'),
        test_texts='hello',
        test_nullable=1,
    ))
    db.add(TestDataTypes(
        id=2,
        test_integers=100,
        test_floats=100.1,
        test_blobs=bytes.fromhex('c0ffeef00d'),
        test_texts='world',
        test_nullable=1,
    ))
    for i in range(3, 10):
        db.add(TestDataTypes(
            id=i,
            test_integers=-100,
            test_floats=-100.1,
            test_blobs=bytes.fromhex('1337c0de'),
            test_texts='hello world',
            test_nullable=1,
        ))
    db.add(TestDataTypes(
        id=10,
        test_integers=1,
        test_floats=-1.1,
        test_blobs=bytes.fromhex('deadbeef'),
        test_texts='hello',
        test_nullable=None,
    ))
    db.commit()


def create_unicode_table(db):
    db.query(TestUnicode).delete()
    db.add(TestUnicode(
        id=1,
        test_texts='Ħ€ȽȽ©',
    ))
    db.add(TestUnicode(
        id=2,
        test_texts='ŴǑȒȽƉ',
    ))
    for i in range(3, 10):
        db.add(TestUnicode(
            id=i,
            test_texts='Ħ€ȽȽ© ŴǑȒȽƉ',
        ))
    db.add(TestUnicode(
        id=10,
        test_texts='Ħ€ȽȽ©',
    ))
    db.commit()


def create_int_optimization_tables(db):
    db.query(TestIntOptimizations).delete()
    for i, v in enumerate([int(random.gauss(100, 10)) for _ in range(100)], 1):
        db.add(TestIntOptimizations(
            id=i,
            norm_dist=v
        ))
    db.commit()


def main():
    clear_tables()
    for db in DBS.values():
        create_data_types_table(db)
        create_unicode_table(db)
        create_int_optimization_tables(db)


if __name__ == '__main__':
    main()
