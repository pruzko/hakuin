import json
import os
import sys

from hakuin.dbms import SQLite
from hakuin import Exfiltrator, OfflineRequester


DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
FILE_GDB = os.path.join(DIR_ROOT, 'experiments', 'generic_db', 'db.json')



def main():
    assert len(sys.argv) in [1, 3], 'python3 experiment_generic_db_offline.py [table> <column>]'

    requester = OfflineRequester()
    dbms = SQLite()
    exf = Exfiltrator(requester, dbms)

    if len(sys.argv) == 1:
        rpc = {}

        with open(FILE_GDB) as f:
            db = json.load(f)

        # copy the names of tables and columns
        for table, rows in db.items():
            rpc[table] = {}
            for column in rows[0]:
                rpc[table][column] = None

        # measure rpc
        for table, columns in rpc.items():
            for column in columns:
                res = exf.exfiltrate_text_data(table, column)
                res_len = len(''.join(res))
                rpc[table][column] = requester.n_queries / len(''.join(res))
                requester.n_queries = 0

        print(json.dumps(rpc, indent=4))
    else:
        res = exf.exfiltrate_text_data(sys.argv[1], sys.argv[2])
        print(requester.n_queries / len(''.join(res)))


if __name__ == '__main__':
    main()