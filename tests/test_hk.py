import copy
import json
import os
import subprocess
import unittest
from abc import ABCMeta, abstractmethod
from itertools import chain

from hakuin.utils import DIR_ROOT



DB_DATA = {
    'test_data_types': {
        'id': list(range(1, 11)),
        'test_integers': [1, 100, -100, -100, -100, -100, -100, -100, -100, 1],
        'test_floats': [
            1.1, 100.1, -100.1, -100.1, -100.1, -100.1, -100.1, -100.1, -100.1, -0.0123,
        ],
        'test_blobs': [
            '0xdeadbeef', '0xc0ffeef00d', '0x1337c0de', '0x1337c0de', '0x1337c0de', '0x1337c0de',
            '0x1337c0de', '0x1337c0de', '0x1337c0de', '0xdeadbeef',
        ],
        'test_texts': [
            'hello', 'world', 'hello world', 'hello world', 'hello world', 'hello world',
            'hello world', 'hello world', 'hello world', 'hello',
        ],
        'test_nullable': [1] * 9 + [None],
    },
    'test_int_optimizations': {
        'id': list(range(1, 101)),
        'norm_dist': [
            98, 98, 98, 107, 98, 85, 103, 97, 97, 101, 102, 111, 106, 101, 92, 89, 102, 113, 100,
            98, 105, 85, 96, 104, 108, 97, 103, 102, 107, 88, 105, 84, 73, 93, 90, 108, 106, 87,
            108, 89, 99, 97, 101, 108, 106, 103, 106, 104, 93, 92, 95, 104, 97, 123, 91, 89, 107,
            114, 105, 108, 114, 99, 85, 94, 109, 85, 100, 102, 96, 107, 105, 123, 106, 93, 94, 91,
            109, 94, 99, 107, 92, 97, 81, 89, 94, 104, 111, 99, 102, 101, 110, 108, 102, 89, 109,
            103, 112, 99, 119, 96,
        ]
    },
    'Ħ€ȽȽ©': {
        'id': list(range(1, 11)),
        'ŴǑȒȽƉ': [
            'Ħ€ȽȽ©', 'ŴǑȒȽƉ', 'Ħ€ȽȽ© ŴǑȒȽƉ', 'Ħ€ȽȽ© ŴǑȒȽƉ', 'Ħ€ȽȽ© ŴǑȒȽƉ', 'Ħ€ȽȽ© ŴǑȒȽƉ',
            'Ħ€ȽȽ© ŴǑȒȽƉ', 'Ħ€ȽȽ© ŴǑȒȽƉ', 'Ħ€ȽȽ© ŴǑȒȽƉ', 'Ħ€ȽȽ©',
        ]
    },
}



class HKTests(metaclass=ABCMeta):
    HK_PATH = os.path.abspath(os.path.join(DIR_ROOT, '..', 'hk.py'))
    DBMS = None
    IP = None
    PORT = None


    def run_hk(self, args={}):
        args = {
            'url': f'http://{self.IP}:{self.PORT}/{self.DBMS}?query=({{query}})',
            '-D': self.DBMS,
            '-i': 'status:200',
            **args,
        }

        arg_list = [args.pop('url')]
        arg_list.extend(chain.from_iterable(args.items()))

        res = subprocess.run(['python', self.HK_PATH, *arg_list], capture_output=True, text=True)
        self.assertEqual(
            res.returncode, 0, msg=f'Process failed with code {res.returncode} - {res.stderr}'
        )
        json_res = json.loads(res.stdout)
        return json_res['stats']['n_requests'], json_res['data']


    def assertEqualUnordered(self, o1, o2):
        def sort_lists(o):
            if isinstance(o, dict):
                return {k: sort_lists(v) for k, v in o.items()}
            if isinstance(o, list):
                return sorted(sort_lists(x) for x in o)
            return o

        self.assertEqual(sort_lists(o1), sort_lists(o2))


    @abstractmethod
    def test_schema_names(self):
        raise NotImplementedError


    def test_meta(self):
        n_requests, data = self.run_hk(args={'-x': 'meta'})

        db_data = copy.deepcopy(DB_DATA)
        for table, colunms in db_data.items():
            db_data[table] = list(colunms)

        self.assertEqual(n_requests, 632)
        self.assertEqualUnordered(data, db_data)


    def test_table_names(self):
        n_requests, data = self.run_hk(args={'-x': 'tables'})
        self.assertEqual(n_requests, 230)
        self.assertEqualUnordered(data, list(DB_DATA))


    def test_column_names(self):
        n_requests, data = self.run_hk(args={'-x': 'columns', '-t': 'test_data_types'})
        self.assertEqual(n_requests, 175)
        self.assertEqualUnordered(data, list(DB_DATA['test_data_types']))

        n_requests, data = self.run_hk(args={'-x': 'columns', '-t': 'test_int_optimizations'})
        self.assertEqual(n_requests, 45)
        self.assertEqualUnordered(data, list(DB_DATA['test_int_optimizations']))

        n_requests, data = self.run_hk(args={'-x': 'columns', '-t': 'Ħ€ȽȽ©'})
        self.assertEqual(n_requests, 182)
        self.assertEqualUnordered(data, list(DB_DATA['Ħ€ȽȽ©']))


    def test_column_integers(self):
        n_requests, data = self.run_hk(args={'-t': 'test_data_types', '-c': 'id'})
        self.assertEqual(n_requests, 35)
        self.assertEqual(data, list(DB_DATA['test_data_types']['id']))

        n_requests, data = self.run_hk(args={'-t': 'test_data_types', '-c': 'test_integers'})
        self.assertEqual(n_requests, 70)
        self.assertEqual(data, list(DB_DATA['test_data_types']['test_integers']))


    def test_column_integers(self):
        n_requests, data = self.run_hk(args={'-t': 'test_data_types', '-c': 'test_floats'})
        self.assertEqual(n_requests, 113)
        self.assertEqual(data, list(DB_DATA['test_data_types']['test_floats']))


    def test_column_floats(self):
        n_requests, data = self.run_hk(args={'-t': 'test_data_types', '-c': 'test_blobs'})
        self.assertEqual(n_requests, 194)
        self.assertEqual(data, list(DB_DATA['test_data_types']['test_blobs']))


    def test_column_texts(self):
        n_requests, data = self.run_hk(args={'-t': 'test_data_types', '-c': 'test_texts'})
        self.assertEqual(n_requests, 160)
        self.assertEqual(data, list(DB_DATA['test_data_types']['test_texts']))


    def test_column_nullable(self):
        n_requests, data = self.run_hk(args={'-t': 'test_data_types', '-c': 'test_nullable'})
        self.assertEqual(n_requests, 51)
        self.assertEqual(data, list(DB_DATA['test_data_types']['test_nullable']))


    def test_column_unicode(self):
        n_requests, data = self.run_hk(args={'-t': 'Ħ€ȽȽ©', '-c': 'ŴǑȒȽƉ'})
        self.assertEqual(n_requests, 300)
        self.assertEqual(data, list(DB_DATA['Ħ€ȽȽ©']['ŴǑȒȽƉ']))


    def test_int_optimizations(self):
        n_requests, data = self.run_hk(args={'-t': 'test_int_optimizations', '-c': 'norm_dist'})
        self.assertEqual(n_requests, 614)
        self.assertEqual(data, list(DB_DATA['test_int_optimizations']['norm_dist']))



class SQLiteHKTests(HKTests, unittest.TestCase):
    DBMS = 'sqlite'
    IP = '192.168.122.191'
    PORT = 5000

    def test_schema_names(self):
        n_requests, data = self.run_hk(args={'-x': 'schemas'})
        self.assertEqual(n_requests, 16)
        self.assertEqual(data, ['main'])



class MySQLHKTests(HKTests, unittest.TestCase):
    DBMS = 'mysql'
    IP = '192.168.122.191'
    PORT = 5000

    def test_schema_names(self):
        n_requests, data = self.run_hk(args={'-x': 'schemas'})
        self.assertEqual(n_requests, 114)
        self.assertEqual(data, ['information_schema', 'performance_schema', 'hakuindb'])



class PostgresHKTests(HKTests, unittest.TestCase):
    DBMS = 'postgres'
    IP = '192.168.122.191'
    PORT = 5000

    def test_schema_names(self):
        n_requests, data = self.run_hk(args={'-x': 'schemas'})
        self.assertEqual(n_requests, 55)
        self.assertEqual(data, ['public', 'information_schema', 'pg_catalog'])



class MsSQLHKTests(HKTests, unittest.TestCase):
    DBMS = 'mssql'
    IP = '192.168.122.191'
    PORT = 5000

    def test_schema_names(self):
        n_requests, data = self.run_hk(args={'-x': 'schemas'})
        self.assertEqual(n_requests, 448)
        self.assertEqual(data, [
            'dbo', 'guest', 'information_schema', 'sys', 'db_owner', 'db_accessadmin',
            'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader',
            'db_datawriter', 'db_denydatareader', 'db_denydatawriter'
        ])
