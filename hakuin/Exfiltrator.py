import hakuin
import hakuin.optimizers as optim
import hakuin.collectors as collect
from hakuin.utils import CHARSET_SCHEMA



class Exfiltrator:
    def __init__(self, requester, dbms):
        self.requester = requester
        self.dbms = dbms


    def exfiltrate_tables(self, mode='model_search'):
        allowed_modes = ['binary_search', 'model_search']
        assert mode in allowed_modes, f'Invalid mode: {mode} not in {allowed_modes}'

        ctx = optim.Context(None, None, None, None)
        n_rows = optim.NumericBinarySearch(
            self.requester,
            self.dbms.queries.count_tables,
            upper=8
        ).run(ctx)

        if mode == 'binary_search':
            return collect.BinaryTextCollector(
                self.requester,
                self.dbms.queries.char_tables,
                charset=CHARSET_SCHEMA,
            ).run(ctx, n_rows)
        else:
            model = hakuin.get_model_tables()
            return collect.ModelTextCollector(
                self.requester,
                self.dbms.queries.char_tables,
                model,
                charset=CHARSET_SCHEMA,
            ).run(ctx, n_rows)


    def exfiltrate_columns(self, table, mode='model_search'):
        allowed_modes = ['binary_search', 'model_search']
        assert mode in allowed_modes, f'Invalid mode: {mode} not in {allowed_modes}'

        ctx = optim.Context(table, None, None, None)
        n_rows = optim.NumericBinarySearch(
            self.requester,
            self.dbms.queries.count_columns,
            upper=8
        ).run(ctx)

        if mode == 'binary_search':
            return collect.BinaryTextCollector(
                self.requester,
                self.dbms.queries.char_columns,
                charset=CHARSET_SCHEMA,
            ).run(ctx, n_rows)
        else:
            model = hakuin.get_model_columns()
            return collect.ModelTextCollector(
                self.requester,
                self.dbms.queries.char_columns,
                model,
                charset=CHARSET_SCHEMA,
            ).run(ctx, n_rows)


    def exfiltrate_metadata(self, table, column):
        ctx = optim.Context(table, column, None, None)

        d_type = optim.BinarySearch(
            self.requester,
            self.dbms.queries.meta_type,
            values=self.dbms.DATA_TYPES,
        ).run(ctx)

        return {
            'type': d_type,
            'nullable': self.requester.request(ctx, self.dbms.queries.meta_is_nullable(ctx)),
            'pk': self.requester.request(ctx, self.dbms.queries.meta_is_pk(ctx)),
        }


    def exfiltrate_schema(self, mode='model_search', metadata=False):
        allowed_modes = ['binary_search', 'model_search']
        assert mode in allowed_modes, f'Invalid mode: {mode} not in {allowed_modes}'

        schema = {}
        for table in self.exfiltrate_tables(mode):
            schema[table] = {}
            for column in self.exfiltrate_columns(table, mode):
                md = self.exfiltrate_metadata(table, column) if metadata else None
                schema[table][column] = md

        return schema


    def exfiltrate_text_data(self, table, column, mode='dynamic_search', charset=None, n_rows=None, n_rows_guess=128):
        allowed_modes = ['binary_search', 'adaptive_search', 'unigram_search', 'dynamic_search']
        assert mode in allowed_modes, f'Invalid mode: {mode} not in {allowed_modes}'

        ctx = optim.Context(table, column, None, None)

        if n_rows is None:
            n_rows = optim.NumericBinarySearch(
                self.requester,
                self.dbms.queries.count_rows,
                upper=n_rows_guess
            ).run(ctx)

        if mode == 'binary_search':
            return collect.BinaryTextCollector(
                self.requester,
                self.dbms.queries.char_rows,
                charset=charset,
            ).run(ctx, n_rows)
        elif mode in ['adaptive_search', 'unigram_search']:
            ngram = 5 if mode == 'adaptive_search' else 1
            return collect.AdaptiveTextCollector(
                self.requester,
                self.dbms.queries.char_rows,
                model=hakuin.Model.make_clean(ngram),
                charset=charset,
            ).run(ctx, n_rows)
        else:
            return collect.DynamicTextCollector(
                self.requester,
                self.dbms.queries.char_rows,
                self.dbms.queries.string_rows,
                charset=charset,
            ).run(ctx, n_rows)
