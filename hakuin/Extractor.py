import hakuin
import hakuin.search_algorithms as search_alg
import hakuin.collectors as collect
from hakuin.utils import CHARSET_SCHEMA



class Extractor:
    '''Class for extracting DB.'''
    def __init__(self, requester, dbms):
        '''Constructor.

        Params:
            requester (Requester): Requester instance used to inject queries
            dbms (DBMS): DBMS instance used to construct queries
        '''
        self.requester = requester
        self.dbms = dbms


    def extract_table_names(self, strategy='model'):
        '''Extracts table names.

        Params:
            strategy (str): 'binary' for binary search or 'model' for pre-trained
                            models with Huffman trees

        Returns:
            list: List of extracted table names
        '''
        allowed = ['binary', 'model']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        ctx = search_alg.Context(None, None, None, None)

        n_rows = search_alg.IntExponentialSearch(
            self.requester,
            self.dbms.count_tables,
            upper=8
        ).run(ctx)

        if strategy == 'binary':
            return collect.BinaryTextCollector(
                self.requester,
                self.dbms.char_tables,
                charset=CHARSET_SCHEMA,
            ).run(ctx, n_rows)
        else:
            return collect.ModelTextCollector(
                self.requester,
                self.dbms.char_tables,
                model=hakuin.get_model_tables(),
                charset=CHARSET_SCHEMA,
            ).run(ctx, n_rows)


    def extract_column_names(self, table, strategy='model'):
        '''Extracts table column names.

        Params:
            table (str): table name
            strategy (str): 'binary' for binary search or 'model' for pre-trained
                        models with Huffman trees

        Returns:
            list: List of extracted column names
        '''
        allowed = ['binary', 'model']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        ctx = search_alg.Context(table, None, None, None)

        n_rows = search_alg.IntExponentialSearch(
            self.requester,
            self.dbms.count_columns,
            upper=8
        ).run(ctx)

        if strategy == 'binary':
            return collect.BinaryTextCollector(
                self.requester,
                self.dbms.char_columns,
                charset=CHARSET_SCHEMA,
            ).run(ctx, n_rows)
        else:
            return collect.ModelTextCollector(
                self.requester,
                self.dbms.char_columns,
                model=hakuin.get_model_columns(),
                charset=CHARSET_SCHEMA,
            ).run(ctx, n_rows)


    def extract_column_metadata(self, table, column):
        '''Extracts column metadata (data type, nullable, and primary key).

        Params:
            table (str): table name
            column (str): column name

        Returns:
            dict: column metadata
        '''
        ctx = search_alg.Context(table, column, None, None)

        d_type = search_alg.BinarySearch(
            self.requester,
            self.dbms.meta_type,
            values=self.dbms.DATA_TYPES,
        ).run(ctx)

        return {
            'type': d_type,
            'nullable': self.requester.request(ctx, self.dbms.meta_is_nullable(ctx)),
            'pk': self.requester.request(ctx, self.dbms.meta_is_pk(ctx)),
        }


    def extract_schema(self, strategy='model', metadata=False):
        '''Extracts schema.

        Params:
            strategy (str): 'binary' for binary search or 'model' for pre-trained
                            models with Huffman trees
            metadata (bool): if set, the metadata will be extracted as well

        Returns:
            dict: schema
        '''
        allowed = ['binary', 'model']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        schema = {}
        for table in self.extract_table_names(strategy):
            schema[table] = {}
            for column in self.extract_column_names(table, strategy):
                md = self.extract_column_metadata(table, column) if metadata else None
                schema[table][column] = md

        return schema


    def extract_column(self, table, column, strategy='dynamic', charset=None, n_rows=None, n_rows_guess=128):
        '''Extracts text column.

        Params:
            table (str): table name
            column (str): column name
            strategy (str): 'binary' for binary search or
                        'unigram' for adaptive unigram model with Huffman trees or
                        'fivegram' for adaptive five-gram model with Huffman trees or
                        'dynamic' for dynamically choosing the best search strategy and
                                         opportunistically guessing strings
            charset (list|None): list of possible characters
            n_rows (int|None): number of rows
            n_rows_guess (int|None): approximate number of rows when 'n_rows' is not set

        Returns:
            list: List of strings in the column
        '''
        allowed = ['binary', 'unigram', 'fivegram', 'dynamic']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        ctx = search_alg.Context(table, column, None, None)

        if n_rows is None:
            n_rows = search_alg.IntExponentialSearch(
                self.requester,
                self.dbms.count_rows,
                upper=n_rows_guess
            ).run(ctx)

        if strategy == 'binary':
            return collect.BinaryTextCollector(
                self.requester,
                self.dbms.char_rows,
                charset=charset,
            ).run(ctx, n_rows)
        elif strategy in ['unigram', 'fivegram']:
            ngram = 1 if strategy == 'unigram' else 5
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
