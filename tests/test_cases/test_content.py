import unittest

from tests import HKTest



class TestContent(HKTest):
    pass


for dbms in HKTest.CONFIG.get('dbms', ['mssql', 'mysql', 'oracledb', 'psql', 'sqlite']):
    ip = HKTest.CONFIG[dbms]['ip']
    port = HKTest.CONFIG[dbms]['port']
    test_content = HKTest.generate_test(
        hk_args={
            'url': f'http://{ip}:{port}/{dbms}?query=({{query}})',
            '-D': dbms,
            '-i': 'status:200',
        },
        result={
            'Ħ€ȽȽ©': {
                'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                'ŴǑȒȽƉ': [
                    'Ħ€ȽȽ©', 'ŴǑȒȽƉ', 'Ħ€ȽȽ© ŴǑȒȽƉ', 'Ħ€ȽȽ© ŴǑȒȽƉ', 'Ħ€ȽȽ© ŴǑȒȽƉ', 'Ħ€ȽȽ© ŴǑȒȽƉ', 'Ħ€ȽȽ© ŴǑȒȽƉ', 'Ħ€ȽȽ© ŴǑȒȽƉ',
                    'Ħ€ȽȽ© ŴǑȒȽƉ', 'Ħ€ȽȽ©'
                ]
            },
            'test_data_types': {
                'id': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                'test_integers': [1, 100, -100, -100, -100, -100, -100, -100, -100, 1],
                'test_floats': [1.1, 100.1, -100.1, -100.1, -100.1, -100.1, -100.1, -100.1, -100.1, -1.1],
                'test_blobs': [
                    'deadbeef', 'c0ffeef00d', '1337c0de', '1337c0de', '1337c0de', '1337c0de', '1337c0de', '1337c0de', '1337c0de',
                    'deadbeef'
                ],
                'test_texts': [
                    'hello', 'world', 'hello world', 'hello world', 'hello world', 'hello world', 'hello world', 'hello world',
                    'hello world', 'hello'
                ],
                'test_nullable': [1, 1, 1, 1, 1, 1, 1, 1, 1, None],
            }
        },
        n_requests=2088,
        order_important=True,
    )
    setattr(TestContent, f'test_content_{dbms}', test_content)