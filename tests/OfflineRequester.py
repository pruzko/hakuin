import os
import sqlite3

from hakuin import HKRequester



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_DBS = os.path.abspath(os.path.join(DIR_FILE, 'dbs'))



class OfflineRequester(HKRequester):
    '''Offline requester for testing purposes.'''
    DB_FILE = None

    def __init__(self):
        super().__init__()
        self.db = None
        self.cursor = None


    async def initialize(self):
        assert os.path.exists(self.DB_FILE), f'DB not found: {self.DB_FILE}'
        self.db = sqlite3.connect(self.DB_FILE)
        self.cursor = self.db.cursor()


    async def cleanup(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None

        if self.db:
            self.db.close()
            self.db = None


    async def request(self, ctx, query):
        self.n_requests += 1
        query = f'SELECT cast(({query}) as bool)'
        return bool(self.db.execute(query).fetchone()[0])



class MetaOfflineRequester(OfflineRequester):
    DB_FILE = f'{DIR_DBS}/test_efficiency_meta.sqlite'


class ContentOfflineRequester(OfflineRequester):
    DB_FILE = f'{DIR_DBS}/test_efficiency_content.sqlite'