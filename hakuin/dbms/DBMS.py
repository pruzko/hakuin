from abc import ABCMeta, abstractmethod

from hakuin.dbms.queries import SQLiteQueries



class DBMS(metaclass=ABCMeta):
    DATA_TYPES = []


    def __init__(self, queries):
        self.queries = queries



class SQLite(DBMS):
    DATA_TYPES = ['INTEGER', 'TEXT', 'REAL', 'NUMERIC', 'BLOB']


    def __init__(self):
        super().__init__(SQLiteQueries())
