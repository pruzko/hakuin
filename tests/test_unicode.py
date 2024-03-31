import asyncio
import logging

import hakuin
from hakuin import Extractor
from hakuin.dbms import SQLite

from OfflineRequester import OfflineRequester



logging.basicConfig(level=logging.INFO)



async def main():
    requester = OfflineRequester(db='unicode', verbose=False)
    ext = Extractor(requester=requester, dbms=SQLite())

    res = await ext.extract_meta(strategy='binary')
    print(res)

    res = await ext.extract_column_text(table='Ħ€ȽȽ©', column='ŴǑȒȽƉ', strategy='dynamic')
    print(res)
    


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
