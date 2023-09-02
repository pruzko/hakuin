import json

from hakuin.dbms import SQLite
from hakuin import Exfiltrator

from OfflineRequester import OfflineRequester



def main():
    requester = OfflineRequester(db='large_schema')
    exf = Exfiltrator(requester=requester, dbms=SQLite())

    res = exf.exfiltrate_schema(mode='model_search')
    print(json.dumps(res, indent=4))

    res_len = sum([len(table) for table in res])
    res_len += sum([len(column) for table, columns in res.items() for column in columns])
    print('Total requests:', requester.n_queries)
    print('Average RPC:', requester.n_queries / res_len)


if __name__ == '__main__':
    main()