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


# Expected RPCs:
# {
#     "users": {
#         "username": [
#             42125,
#             5.738319030104891
#         ],
#         "first_name": [
#             27874,
#             4.878193909695485
#         ],
#         "last_name": [
#             32703,
#             5.344500735414283
#         ],
#         "sex": [
#             1608,
#             0.3216
#         ],
#         "email": [
#             78084,
#             3.750612421345886
#         ],
#         "password": [
#             137115,
#             4.28484375
#         ],
#         "address": [
#             86877,
#             2.19480585099664
#         ]
#     },
#     "posts": {
#         "text": [
#             409212,
#             4.311533963397288
#         ]
#     },
#     "comments": {
#         "text": [
#             346192,
#             3.9184153933220145
#         ]
#     },
#     "products": {
#         "name": [
#             491018,
#             3.87250386447522
#         ],
#         "category": [
#             6720,
#             0.42969499328601574
#         ],
#         "description": [
#             966265,
#             3.2258080669822595
#         ]
#     }
# }
