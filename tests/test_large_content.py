import json
import logging
import os
import sys

from hakuin.dbms import SQLite
from hakuin import Exfiltrator

from OfflineRequester import OfflineRequester



logging.basicConfig(level=logging.INFO)


DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_DBS = os.path.abspath(os.path.join(DIR_FILE, 'dbs'))
FILE_LARGE_CONTENT_JSON = os.path.join(DIR_DBS, 'large_content.json')


def main():
    assert len(sys.argv) in [1, 3], 'python3 experiment_generic_db_offline.py [table> <column>]'

    requester = OfflineRequester(db='large_content')
    exf = Exfiltrator(requester=requester, dbms=SQLite())

    if len(sys.argv) == 3:
        res = exf.exfiltrate_text_data(sys.argv[1], sys.argv[2])
        print('Total requests:', requester.n_queries)
        print('Average RPC:', requester.n_queries / len(''.join(res)))
    else:
        rpc = {}

        with open(FILE_LARGE_CONTENT_JSON) as f:
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
                col_rpc = requester.n_queries / len(''.join(res))
                rpc[table][column] = (requester.n_queries, col_rpc)
                requester.n_queries = 0

        print(json.dumps(rpc, indent=4))


if __name__ == '__main__':
    main()