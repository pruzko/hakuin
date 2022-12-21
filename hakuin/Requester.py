import os
from abc import ABCMeta, abstractmethod

DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
DIR_GENERIC_DB = os.path.join(DIR_ROOT, 'experiments', 'generic_db')
DIR_DBANSWERS = os.path.join(DIR_ROOT, 'experiments', 'dbanswers')



class Requester(metaclass=ABCMeta):
    @abstractmethod
    def request(self, ctx, query):
        raise NotImplementedError()



class OfflineRequester(Requester):
    def __init__(self, db='generic_db'):
        assert db in ['generic_db', 'schemas']
        import sqlite3
        if db == 'generic_db':
            self.db = sqlite3.connect(os.path.join(DIR_GENERIC_DB, 'db.sqlite')).cursor()
        else:
            self.db = sqlite3.connect(os.path.join(DIR_DBANSWERS, 'schemas.sqlite')).cursor()
        self.n_queries = 0


    def request(self, ctx, query):
        self.n_queries += 1
        query = f'SELECT cast(({query}) as bool)'
        return bool(self.db.execute(query).fetchone()[0])


    def reset(self):
        self.n_queries = 0
