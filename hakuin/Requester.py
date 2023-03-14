import os
from abc import ABCMeta, abstractmethod

DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
FILE_SDB = os.path.join(DIR_ROOT, 'experiments', 'schema_db', 'db.sqlite')
FILE_GDB = os.path.join(DIR_ROOT, 'experiments', 'generic_db', 'db.sqlite')



class Requester(metaclass=ABCMeta):
    @abstractmethod
    def request(self, ctx, query):
        raise NotImplementedError()



class OfflineRequester(Requester):
    def __init__(self, db='generic_db'):
        import sqlite3

        assert db in ['generic_db', 'schema_db']

        if db == 'generic_db':
            self.db = sqlite3.connect(FILE_GDB).cursor()
        else:
            self.db = sqlite3.connect(FILE_SDB).cursor()
        self.n_queries = 0


    def request(self, ctx, query):
        self.n_queries += 1
        query = f'SELECT cast(({query}) as bool)'
        return bool(self.db.execute(query).fetchone()[0])


    def reset(self):
        self.n_queries = 0
