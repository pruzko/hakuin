import os
import sqlite3

from hakuin import Requester



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_DBS = os.path.abspath(os.path.join(DIR_FILE, 'dbs'))



class OfflineRequester(Requester):
    '''Offline requester for testing purposes.'''
    def __init__(self, db):
        '''Constructor.

        Params:
            db (str): name of an .sqlite DB in the "dbs" dir
        '''
        db_file = os.path.join(DIR_DBS, f'{db}.sqlite')
        assert os.path.exists(db_file), f'DB not found: {db_file}'
        self.db = sqlite3.connect(db_file).cursor()
        self.n_queries = 0


    def request(self, ctx, query):
        self.n_queries += 1
        query = f'SELECT cast(({query}) as bool)'
        return bool(self.db.execute(query).fetchone()[0])


    def reset(self):
        self.n_queries = 0
