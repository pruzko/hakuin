import asyncio
import json
import os
import sqlite3
import unittest

from hakuin import Extractor, Requester
from hakuin.utils import DIR_ROOT



DIR_DBS = os.path.abspath(os.path.join(DIR_ROOT, '..', 'tests', 'dbs'))



class OfflineRequester(Requester):
    def __init__(self, db_file):
        super().__init__()
        self.db = None
        self.cursor = None
        self.db_file = db_file


    async def request(self, query, ctx):
        query = f'SELECT cast(({query.render(ctx)}) as bool)'
        return bool(self.db.execute(query).fetchone()[0])


    async def __aenter__(self):
        assert os.path.exists(self.db_file), f'DB not found: {self.db_file}'
        self.db = sqlite3.connect(self.db_file)
        self.cursor = self.db.cursor()
        return self


    async def __aexit__(self, *args, **kwargs):
        if self.cursor:
            self.cursor.close()
            self.cursor = None

        if self.db:
            self.db.close()
            self.db = None



@unittest.skip('Offline tests are disabled.')
class OfflineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(f'{DIR_DBS}/test_meta.json') as f:
            cls.db_data_meta = json.load(f)

        with open(f'{DIR_DBS}/test_content.json') as f:
            cls.db_data_content = json.load(f)


    async def dump_meta(self):
        requester = OfflineRequester(db_file=f'{DIR_DBS}/test_meta.sqlite')
        async with requester:
            ext = Extractor(requester=requester, dbms='sqlite')
            data = await ext.extract_meta()
            n_requests = await requester.n_requests()

        return n_requests, data

    
    async def dump_column(self, table, column):
        requester = OfflineRequester(db_file=f'{DIR_DBS}/test_content.sqlite')
        async with requester:
            ext = Extractor(requester=requester, dbms='sqlite')
            data = await ext.extract_column(table=table, column=column)
            n_requests = await requester.n_requests()

        return n_requests, data


    def test_meta(self):
        n_requests, data = asyncio.run(self.dump_meta())
        self.assertEqual(n_requests, 27376)
        self.assertEqual(data, self.db_data_meta)


    def test_column_users_sex(self):
        n_requests, data = asyncio.run(self.dump_column('users', 'sex'))
        self.assertEqual(n_requests, 1617)
        self.assertEqual(data, self.db_data_content['users']['sex'])


    def test_column_users_address(self):
        n_requests, data = asyncio.run(self.dump_column('users', 'address'))
        self.assertEqual(n_requests, 86821)
        self.assertEqual(data, self.db_data_content['users']['address'])


    def test_column_users_username(self):
        n_requests, data = asyncio.run(self.dump_column('users', 'username'))
        self.assertEqual(n_requests, 42108)
        self.assertEqual(data, self.db_data_content['users']['username'])


    def test_column_users_password(self):
        n_requests, data = asyncio.run(self.dump_column('users', 'password'))
        self.assertEqual(n_requests, 137119)
        self.assertEqual(data, self.db_data_content['users']['password'])
