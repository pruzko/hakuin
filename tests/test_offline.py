import asyncio
import json
import os
import sqlite3
import unittest

from hakuin import Extractor, Requester
from hakuin.utils import DIR_ROOT
from hakuin.exceptions import ServerError



DIR_DBS = os.path.abspath(os.path.join(DIR_ROOT, '..', 'tests', 'dbs'))



class OfflineRequester(Requester):
    DB_PATH = None
    def __init__(self):
        super().__init__()
        self.db = None
        self.cursor = None


    async def request(self, query, ctx):
        query = f'SELECT cast(({query.render(ctx)}) as bool)'
        try:
            return bool(self.db.execute(query).fetchone()[0])
        except sqlite3.OperationalError:
            raise ServerError


    async def __aenter__(self):
        assert os.path.exists(self.DB_PATH), f'DB not found: {self.DB_PATH}'
        self.db = sqlite3.connect(self.DB_PATH)
        self.cursor = self.db.cursor()
        return self


    async def __aexit__(self, *args, **kwargs):
        if self.cursor:
            self.cursor.close()
            self.cursor = None

        if self.db:
            self.db.close()
            self.db = None



class OfflineMetaRequester(OfflineRequester):
    DB_PATH = f'{DIR_DBS}/test_meta.sqlite'


class OfflineContentRequester(OfflineRequester):
    DB_PATH = f'{DIR_DBS}/test_content.sqlite'




# @unittest.skip('Offline tests are disabled.')
class OfflineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open(f'{DIR_DBS}/test_meta.json') as f:
            cls.db_data_meta = json.load(f)

        with open(f'{DIR_DBS}/test_content.json') as f:
            cls.db_data_content = json.load(f)


    async def dump_meta(self):
        requester = OfflineMetaRequester()
        async with requester:
            ext = Extractor(requester=requester, dbms='sqlite')
            data = await ext.extract_meta()
            n_requests = await requester.n_requests()

        return n_requests, data

    
    async def dump_column(self, table, column):
        requester = OfflineContentRequester()
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
