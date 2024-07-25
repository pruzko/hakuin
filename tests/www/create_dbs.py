import os

from sqlalchemy import create_engine, Column, UnicodeText, Integer, Text, LargeBinary, Float
from sqlalchemy.orm import declarative_base, sessionmaker



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_DBS = os.path.abspath(os.path.join(DIR_FILE, '..', 'dbs'))


DB_URIS = {
    # comment out DBMS that are not installed
    'sqlite': f'sqlite:///{os.path.join(DIR_DBS, "test_db.sqlite")}',
    'mysql': 'mysql+pymysql://hakuin:hakuin@localhost/hakuindb',
    'mssql': 'mssql+pyodbc://hakuin:hakuin@localhost/hakuindb?driver=ODBC+Driver+17+for+SQL+Server',
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
    test_text = Column(Text)
    test_nullable = Column(Integer)



class TestUnicode(Base):
    __tablename__ = 'Ħ€ȽȽ©'

    id = Column(Integer, primary_key=True)
    test_text = Column(UnicodeText, name='ŴǑȒȽƉ')



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
        test_text='hello',
        test_nullable=1,
    ))
    db.add(TestDataTypes(
        id=2,
        test_integers=100,
        test_floats=100.1,
        test_blobs=bytes.fromhex('c0ffeef00d'),
        test_text='world',
        test_nullable=1,
    ))
    for i in range(3, 10):
        db.add(TestDataTypes(
            id=i,
            test_integers=-100,
            test_floats=-100.1,
            test_blobs=bytes.fromhex('1337c0de'),
            test_text='hello world',
            test_nullable=1,
        ))
    db.add(TestDataTypes(
        id=10,
        test_integers=1,
        test_floats=-1.1,
        test_blobs=bytes.fromhex('deadbeef'),
        test_text='hello',
        test_nullable=None,
    ))
    db.commit()


def create_unicode_table(db):
    db.query(TestUnicode).delete()
    db.add(TestUnicode(
        id=1,
        test_text='Ħ€ȽȽ©',
    ))
    db.add(TestUnicode(
        id=2,
        test_text='ŴǑȒȽƉ',
    ))
    for i in range(3, 10):
        db.add(TestUnicode(
            id=i,
            test_text='Ħ€ȽȽ© ŴǑȒȽƉ',
        ))
    db.add(TestUnicode(
        id=10,
        test_text='Ħ€ȽȽ©',
    ))
    db.commit()


def main():
    clear_tables()
    for db in DBS.values():
        create_data_types_table(db)
        create_unicode_table(db)


if __name__ == '__main__':
    main()