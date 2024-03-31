import asyncio
import logging

import hakuin
from hakuin import Extractor
from hakuin.dbms import SQLite

from OfflineRequester import OfflineRequester



logging.basicConfig(level=logging.INFO)



async def main():
    requester = OfflineRequester(db='data_types', verbose=False)
    ext = Extractor(requester=requester, dbms=SQLite())

    for column in ['integer', 'text', 'blob', 'real']:
        res = await ext.extract_column(table='data_types', column=column)
        print(res)
    


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
