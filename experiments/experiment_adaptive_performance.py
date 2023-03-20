import json
import os
import sys

from hakuin.dbms import SQLite
from hakuin.collectors import DynamicTextCollector
from hakuin import Exfiltrator, OfflineRequester


DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..'))
FILE_GDB = os.path.join(DIR_ROOT, 'experiments', 'generic_db', 'db.json')



def main():
    requester = OfflineRequester()
    dbms = SQLite()
    exf = Exfiltrator(requester, dbms)

    rpc = {}

    with open(FILE_GDB) as f:
        db = json.load(f)

    # copy the names of tables and columns
    for table, rows in db.items():
        rpc[table] = {}
        for column in rows[0]:
            rpc[table][column] = []

    # hook the _collect_row function to provide additional measurements
    original_collect_row = DynamicTextCollector._collect_row

    def new_collect_row(self, ctx):
        self.requester.n_queries = 0
        s = original_collect_row(self, ctx)
        rpc[ctx.table][ctx.column].append([self.requester.n_queries, len(s)])
        return s

    # set the hook
    DynamicTextCollector._collect_row = new_collect_row

    # measure rpc
    for table, columns in rpc.items():
        for column in columns:
            exf.exfiltrate_text_data(table, column)

    # average out the values over 20 rows and flatten the dictionary
    rpc_flat = {}
    for table, columns in rpc.items():
        for column in columns:
            rpc_flat[f'{table}_{column}'] = []
            for i in range(0, 1000, 20):
                total_queries = sum([rpc[table][column][i + j][0] for j in range(20)])
                total_len = sum([rpc[table][column][i + j][1] for j in range(20)])
                rpc_flat[f'{table}_{column}'].append(round(total_queries / total_len, 2))

    # add baseline
    rpc_flat['baseline'] = [7.00 for x in rpc_flat['users_username']]

    # print csv
    print(','.join(['i'] + list(rpc_flat)))
    for i, vals in enumerate(zip(*rpc_flat.values())):
        print(','.join([str((i + 1) * 20)] + [str(v) for v in vals]))


if __name__ == '__main__':
    main()