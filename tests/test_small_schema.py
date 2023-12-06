import json
import logging

from hakuin.dbms import SQLite
from hakuin import Extractor

from OfflineRequester import OfflineRequester



logging.basicConfig(level=logging.INFO)



def main():
    requester = OfflineRequester(db='large_content')
    ext = Extractor(requester=requester, dbms=SQLite())

    res = ext.extract_schema()
    print(json.dumps(res, indent=4))

    res_len = sum([len(table) for table in res])
    res_len += sum([len(column) for table, columns in res.items() for column in columns])
    print('Total requests:', requester.n_queries)
    print('Average RPC:', requester.n_queries / res_len)


if __name__ == '__main__':
    main()
