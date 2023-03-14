import json
import requests
import sys

from hakuin.dbms import SQLite
from hakuin import Exfiltrator, OfflineRequester


requester = OfflineRequester()
dbms = SQLite()
exf = Exfiltrator(requester, dbms)

assert len(sys.argv) >= 3, 'python3 experiment_generic_db.py <table> <column>'

res = json.dumps(exf.exfiltrate_text_data(sys.argv[1], sys.argv[2]), indent=4)
print(res)
print(requester.n_queries)
