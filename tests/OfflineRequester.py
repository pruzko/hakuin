import os
import sqlite3

from hakuin import Requester



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_DBS = os.path.abspath(os.path.join(DIR_FILE, 'dbs'))



class OfflineRequester(Requester):
    '''Offline requester for testing purposes.'''
    def __init__(self, db, verbose=False):
        '''Constructor.

        Params:
            db (str): name of an .sqlite DB in the "dbs" dir
            verbose (bool): flag for verbous prints
        '''
        db_file = os.path.join(DIR_DBS, f'{db}.sqlite')
        assert os.path.exists(db_file), f'DB not found: {db_file}'
        self.db = sqlite3.connect(db_file).cursor()
        self.verbose = verbose
        self.n_queries = 0


    def request(self, ctx, query):
        self.n_queries += 1
        query = f'SELECT cast(({query}) as bool)'

        res = bool(self.db.execute(query).fetchone()[0])

        if self.verbose:
            print(f'"{ctx.buffer}"\t{res}\t{query}')

        return res



    def reset(self):
        self.n_queries = 0
