import json
import logging

import hakuin
from hakuin import Extractor
from hakuin.dbms import SQLite

from OfflineRequester import OfflineRequester



logging.basicConfig(level=logging.INFO)



def main():
    requester = OfflineRequester(db='data_types', verbose=False)
    ext = Extractor(requester=requester, dbms=SQLite())

    # res = ext.extract_schema(strategy='binary')
    # print(res)

    res = ext.extract_column_int('data_types', 'integer')
    


if __name__ == '__main__':
    main()
