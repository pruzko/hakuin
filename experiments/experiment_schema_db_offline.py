from hakuin.dbms import SQLite
from hakuin import Exfiltrator, OfflineRequester



def main():
    requester = OfflineRequester(db='schema_db')
    dbms = SQLite()
    exf = Exfiltrator(requester, dbms)

    res = exf.exfiltrate_schema(mode='model_search')
    res_len = sum([len(table) for table in res])
    res_len += sum([len(column) for table, columns in res.items() for column in columns])
    print(requester.n_queries)
    print(requester.n_queries / res_len)


if __name__ == '__main__':
    main()