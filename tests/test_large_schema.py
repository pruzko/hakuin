import json
import logging

from hakuin.dbms import SQLite
from hakuin import Extractor

from OfflineRequester import OfflineRequester



logging.basicConfig(level=logging.INFO)



def main():
    requester = OfflineRequester(db='large_schema', verbose=False)
    ext = Extractor(requester=requester, dbms=SQLite())

    res = ext.extract_schema(strategy='model')
    print(json.dumps(res, indent=4))

    res_len = sum([len(table) for table in res])
    res_len += sum([len(column) for table, columns in res.items() for column in columns])
    print('Total requests:', requester.n_queries)
    print('Average RPC:', requester.n_queries / res_len)


if __name__ == '__main__':
    main()


# Expected results:
# Total requests: 27376
# Average RPC: 2.2098805295447206