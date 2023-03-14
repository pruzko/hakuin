import json
import requests

from hakuin.dbms import SQLite
from hakuin import Exfiltrator, OfflineRequester


requester = OfflineRequester(db='schema_db')
dbms = SQLite()
exf = Exfiltrator(requester, dbms)

res = json.dumps(exf.exfiltrate_schema(mode='model_search'), indent=4)
print(res)
print(requester.n_queries)
