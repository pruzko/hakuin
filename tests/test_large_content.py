import json
import logging
import os
import sys

from hakuin.dbms import SQLite
from hakuin import Extractor

from OfflineRequester import OfflineRequester



logging.basicConfig(level=logging.INFO)


DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_DBS = os.path.abspath(os.path.join(DIR_FILE, 'dbs'))
FILE_LARGE_CONTENT_JSON = os.path.join(DIR_DBS, 'large_content.json')


def main():
    assert len(sys.argv) in [1, 3], 'python3 experiment_generic_db_offline.py [table> <column>]'

    requester = OfflineRequester(db='large_content', verbose=False)
    ext = Extractor(requester=requester, dbms=SQLite())

    if len(sys.argv) == 3:
        res = ext.extract_column_text(sys.argv[1], sys.argv[2])
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
                res = ext.extract_column_text(table, column)
                res_len = len(''.join(res))
                col_rpc = requester.n_queries / len(''.join(res))
                rpc[table][column] = (requester.n_queries, col_rpc)
                requester.n_queries = 0

        print(json.dumps(rpc, indent=4))


if __name__ == '__main__':
    main()


# Expected RPCs:
# {
#     "users": {
#         "username": [
#             42125,
#             5.738319030104891
#         ],
#         "first_name": [
#             27902,
#             4.883094154707735
#         ],
#         "last_name": [
#             32702,
#             5.344337310017977
#         ],
#         "sex": [
#             1609,
#             0.3218
#         ],
#         "email": [
#             78139,
#             3.7532542389163743
#         ],
#         "password": [
#             137116,
#             4.284875
#         ],
#         "address": [
#             86873,
#             2.1947047975140843
#         ]
#     },
#     "posts": {
#         "text": [
#             409303,
#             4.312492756371759
#         ]
#     },
#     "comments": {
#         "text": [
#             346374,
#             3.9204753820033957
#         ]
#     },
#     "products": {
#         "name": [
#             491175,
#             3.873742073882457
#         ],
#         "category": [
#             6753,
#             0.4318051026280453
#         ],
#         "description": [
#             966310,
#             3.2259582963324007
#         ]
#     }
# }
