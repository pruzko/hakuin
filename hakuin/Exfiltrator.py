import hakuin
import hakuin.search_algorithms as optim
import hakuin.collectors as collect
from hakuin.utils import CHARSET_SCHEMA



class Exfiltrator:
    '''Class for extracting DB data.'''
    def __init__(self, requester, dbms):
        '''Constructor.

        Params:
            requester (Requester): Requester instance used to inject queries
            dbms (DBMS): DBMS instance used to construct queries
        '''
        self.requester = requester
        self.dbms = dbms


    def exfiltrate_tables(self, mode='model_search'):
        '''Extracts table names.

        Params:
            mode (str): 'binary_search' for binary search or
                        'model_search' for pre-trained models with Huffman trees

        Returns:
            list: List of inferred table names
        '''
        allowed_modes = ['binary_search', 'model_search']
        assert mode in allowed_modes, f'Invalid mode: {mode} not in {allowed_modes}'

        ctx = optim.Context(None, None, None, None)
        n_rows = optim.IntExponentialSearch(
            self.requester,
            self.dbms.count_tables,
            upper=8
        ).run(ctx)

        if mode == 'binary_search':
            return collect.BinaryTextCollector(
                self.requester,
                self.dbms.char_tables,
                charset=CHARSET_SCHEMA,
            ).run(ctx, n_rows)
        else:
            model = hakuin.get_model_tables()
            return collect.ModelTextCollector(
                self.requester,
                self.dbms.char_tables,
                model,
                charset=CHARSET_SCHEMA,
            ).run(ctx, n_rows)


    def exfiltrate_columns(self, table, mode='model_search'):
        '''Extracts table column names.

        Params:
            table (str): table name
            mode (str): 'binary_search' for binary search or
                        'model_search' for pre-trained models with Huffman trees

        Returns:
            list: List of inferred column names
        '''
        allowed_modes = ['binary_search', 'model_search']
        assert mode in allowed_modes, f'Invalid mode: {mode} not in {allowed_modes}'

        ctx = optim.Context(table, None, None, None)
        n_rows = optim.IntExponentialSearch(
            self.requester,
            self.dbms.count_columns,
            upper=8
        ).run(ctx)

        if mode == 'binary_search':
            return collect.BinaryTextCollector(
                self.requester,
                self.dbms.char_columns,
                charset=CHARSET_SCHEMA,
            ).run(ctx, n_rows)
        else:
            model = hakuin.get_model_columns()
            return collect.ModelTextCollector(
                self.requester,
                self.dbms.char_columns,
                model,
                charset=CHARSET_SCHEMA,
            ).run(ctx, n_rows)


    def exfiltrate_metadata(self, table, column):
        '''Extracts column metadata (data type, nullable, and primary key).

        Params:
            table (str): table name
            column (str): column name

        Returns:
            dict: column metadata
        '''
        ctx = optim.Context(table, column, None, None)

        d_type = optim.BinarySearch(
            self.requester,
            self.dbms.meta_type,
            values=self.dbms.DATA_TYPES,
        ).run(ctx)

        return {
            'type': d_type,
            'nullable': self.requester.request(ctx, self.dbms.meta_is_nullable(ctx)),
            'pk': self.requester.request(ctx, self.dbms.meta_is_pk(ctx)),
        }


    def exfiltrate_schema(self, mode='model_search', metadata=False):
        '''Extracts schema.

        Params:
            mode (str): 'binary_search' for binary search or
                        'model_search' for pre-trained models with Huffman trees
            metadata (bool): if set, the metadata will be extracted as well

        Returns:
            dict: schema
        '''
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
        '''Extracts text column.

        Params:
            table (str): table name
            column (str): column name
            mode (str): 'binary_search' for binary search or
                        'adaptive_search' for adaptive five-gram model with Huffman trees or
                        'unigram_search' for adaptive unigram model with Huffman trees or
                        'dynamic_search' for dynamically choosing the best search strategy and
                                         opportunistically guessing strings
            charset (list|None): list of possible characters
            n_rows (int|None): number of rows
            n_rows_guess (int|None): approximate number of rows when 'n_rows' is not set

        Returns:
            list: list of strings in the column
        '''
        allowed_modes = ['binary_search', 'adaptive_search', 'unigram_search', 'dynamic_search']
        assert mode in allowed_modes, f'Invalid mode: {mode} not in {allowed_modes}'

        ctx = optim.Context(table, column, None, None)

        if n_rows is None:
            n_rows = optim.IntExponentialSearch(
                self.requester,
                self.dbms.count_rows,
                upper=n_rows_guess
            ).run(ctx)

        if mode == 'binary_search':
            return collect.BinaryTextCollector(
                self.requester,
                self.dbms.char_rows,
                charset=charset,
            ).run(ctx, n_rows)
        elif mode in ['adaptive_search', 'unigram_search']:
            ngram = 5 if mode == 'adaptive_search' else 1
            return collect.AdaptiveTextCollector(
                self.requester,
                self.dbms.char_rows,
                model=hakuin.Model.make_clean(ngram),
                charset=charset,
            ).run(ctx, n_rows)
        else:
            return collect.DynamicTextCollector(
                self.requester,
                self.dbms.char_rows,
                self.dbms.string_rows,
                charset=charset,
            ).run(ctx, n_rows)
