import asyncio
import json
import logging

from hakuin.dbms import SQLite
from hakuin import Extractor

from OfflineRequester import OfflineRequester



logging.basicConfig(level=logging.INFO)



async def main():
    requester = OfflineRequester(db='large_schema', verbose=False)
    ext = Extractor(requester=requester, dbms=SQLite())

    res = await ext.extract_schema_names(strategy='model')
    print(json.dumps(res, indent=4))

    print(f'RPC: {requester.n_requests / len("".join(res))}')


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
