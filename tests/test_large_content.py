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

    requester = OfflineRequester(db='large_content')
    ext = Extractor(requester=requester, dbms=SQLite())

    if len(sys.argv) == 3:
        res = ext.extract_column(sys.argv[1], sys.argv[2])
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
                res = ext.extract_column(table, column)
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
#             42124,
#             5.738182808881624
#         ],
#         "first_name": [
#             27901,
#             4.882919145957298
#         ],
#         "last_name": [
#             32701,
#             5.344173884621671
#         ],
#         "sex": [
#             1608,
#             0.3216
#         ],
#         "email": [
#             78138,
#             3.7532062058696383
#         ],
#         "password": [
#             137115,
#             4.28484375
#         ],
#         "address": [
#             86872,
#             2.1946795341434453
#         ]
#     },
#     "posts": {
#         "text": [
#             409302,
#             4.312482220185226
#         ]
#     },
#     "comments": {
#         "text": [
#             346373,
#             3.920464063384267
#         ]
#     },
#     "products": {
#         "name": [
#             491174,
#             3.8737341871983344
#         ],
#         "category": [
#             6721,
#             0.42975893599334997
#         ],
#         "description": [
#             966309,
#             3.2259549579023976
#         ]
#     }
# }
