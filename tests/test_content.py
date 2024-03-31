import asyncio
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


async def main():
    assert len(sys.argv) in [1, 3], 'python3 experiment_generic_db_offline.py [table> <column>]'

    requester = OfflineRequester(db='large_content', verbose=False)
    ext = Extractor(requester=requester, dbms=SQLite())

    if len(sys.argv) == 3:
        res = await ext.extract_column_text(sys.argv[1], sys.argv[2])
        print('Total requests:', requester.n_requests)
        print('Average RPC:', requester.n_requests / len(''.join(res)))
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
                res = await ext.extract_column_text(table, column)
                res_len = len(''.join(res))
                col_rpc = requester.n_requests / len(''.join(res))
                rpc[table][column] = (requester.n_requests, col_rpc)
                requester.n_requests = 0

        print(json.dumps(rpc, indent=4))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())


# Expected RPCs:
# {
#     "users": {
#         "username": [
#             42126,
#             5.738455251328157
#         ],
#         "first_name": [
#             27903,
#             4.883269163458173
#         ],
#         "last_name": [
#             32703,
#             5.344500735414283
#         ],
#         "sex": [
#             1610,
#             0.322
#         ],
#         "email": [
#             78140,
#             3.753302271963111
#         ],
#         "password": [
#             137117,
#             4.28490625
#         ],
#         "address": [
#             86874,
#             2.1947300608847233
#         ]
#     },
#     "posts": {
#         "text": [
#             409304,
#             4.312503292558292
#         ]
#     },
#     "comments": {
#         "text": [
#             346375,
#             3.920486700622524
#         ]
#     },
#     "products": {
#         "name": [
#             491176,
#             3.8737499605665793
#         ],
#         "category": [
#             6754,
#             0.4318690453353795
#         ],
#         "description": [
#             966311,
#             3.225961634762404
#         ]
#     }
# }

