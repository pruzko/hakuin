import unittest

from tests import HKTest



class TestMeta(HKTest):
    def test_schemas_sqlite(self):
        res = self.run_hk({
            'url': f'http://{HKTest.CONFIG["sqlite"]["ip"]}:{HKTest.CONFIG["sqlite"]["port"]}/sqlite?query=({{query}})',
            '-D': 'sqlite',
            '-i': 'status:200',
            '-x': 'schemas',
        })
        self.assertEqual(res['stats']['n_requests'], 16)
        self.assertEqual(res['data'], ['main'])


    def test_schemas_mssql(self):
        res = self.run_hk({
            'url': f'http://{HKTest.CONFIG["mssql"]["ip"]}:{HKTest.CONFIG["mssql"]["port"]}/mssql?query=({{query}})',
            '-D': 'mssql',
            '-i': 'status:200',
            '-x': 'schemas',
        })
        self.assertEqual(res['stats']['n_requests'], 702)
        self.assertEqual(res['data'], [
            'dbo', 'guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin',
            'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader',
            'db_datawriter', 'db_denydatareader', 'db_denydatawriter'
        ])


    def test_schemas_mysql(self):
        res = self.run_hk({
            'url': f'http://{HKTest.CONFIG["mysql"]["ip"]}:{HKTest.CONFIG["mysql"]["port"]}/mysql?query=({{query}})',
            '-D': 'mysql',
            '-i': 'status:200',
            '-x': 'schemas',
        })
        self.assertEqual(res['stats']['n_requests'], 114)
        self.assertEqual(res['data'], ['information_schema', 'performance_schema', 'hakuindb'])


    def test_schemas_psql(self):
        res = self.run_hk({
            'url': f'http://{HKTest.CONFIG["psql"]["ip"]}:{HKTest.CONFIG["psql"]["port"]}/psql?query=({{query}})',
            '-D': 'psql',
            '-i': 'status:200',
            '-x': 'schemas',
        })
        self.assertEqual(res['stats']['n_requests'], 55)
        self.assertEqual(res['data'], ['public', 'information_schema', 'pg_catalog'])


for dbms in HKTest.CONFIG.get('dbms', ['mssql', 'mysql', 'oracledb', 'psql', 'sqlite']):
    ip = HKTest.CONFIG[dbms]['ip']
    port = HKTest.CONFIG[dbms]['port']
    test_tables = HKTest.generate_test(
        hk_args={
            'url': f'http://{ip}:{port}/{dbms}?query=({{query}})',
            '-D': dbms,
            '-i': 'status:200',
            '-x': 'tables',
        },
        result=['test_data_types', 'Ħ€ȽȽ©'],
        n_requests=176,
    )
    test_columns = HKTest.generate_test(
        hk_args={
            'url': f'http://{ip}:{port}/{dbms}?query=({{query}})',
            '-D': dbms,
            '-i': 'status:200',
            '-x': 'columns',
            '-t': 'test_data_types',
        },
        result=['id', 'test_integers', 'test_floats', 'test_blobs', 'test_texts', 'test_nullable'],
        n_requests=175,
    )
    test_meta = HKTest.generate_test(
        hk_args={
            'url': f'http://{ip}:{port}/{dbms}?query=({{query}})',
            '-D': dbms,
            '-i': 'status:200',
            '-x': 'meta',
        },
        result={
            'Ħ€ȽȽ©': ['id', 'ŴǑȒȽƉ'],
            'test_data_types': ['id', 'test_integers', 'test_floats', 'test_blobs', 'test_texts', 'test_nullable'],
        },
        n_requests=533,
    )

    setattr(TestMeta, f'test_tables_{dbms}', test_tables)
    setattr(TestMeta, f'test_columns_{dbms}', test_columns)
    setattr(TestMeta, f'test_meta_{dbms}', test_meta)
