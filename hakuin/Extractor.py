import hakuin
import hakuin.search_algorithms as search_alg
import hakuin.collectors as collect



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

        n_rows = search_alg.IntExponentialBinarySearch(
            requester=self.requester,
            query_cb=self.dbms.TablesQueries.rows_count,
            upper=8,
            find_range=True,
        ).run(ctx)

        if strategy == 'binary':
            return collect.BinaryTextCollector(
                requester=self.requester,
                queries=self.dbms.TablesQueries,
            ).run(ctx, n_rows)
        else:
            return collect.ModelTextCollector(
                requester=self.requester,
                queries=self.dbms.TablesQueries,
                model=hakuin.get_model_tables(),
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

        n_rows = search_alg.IntExponentialBinarySearch(
            requester=self.requester,
            query_cb=self.dbms.ColumnsQueries.rows_count,
            upper=8,
            find_range=True,
        ).run(ctx)

        if strategy == 'binary':
            return collect.BinaryTextCollector(
                requester=self.requester,
                queries=self.dbms.ColumnsQueries,
            ).run(ctx, n_rows)
        else:
            return collect.ModelTextCollector(
                requester=self.requester,
                queries=self.dbms.ColumnsQueries,
                model=hakuin.get_model_columns(),
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
            requester=self.requester,
            query_cb=self.dbms.MetaQueries.column_data_type,
            values=self.dbms.DATA_TYPES,
        ).run(ctx)

        return {
            'type': d_type,
            'nullable': self.requester.request(ctx, self.dbms.MetaQueries.column_is_nullable(ctx)),
            'pk': self.requester.request(ctx, self.dbms.MetaQueries.column_is_pk(ctx)),
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


    def extract_column(self, table, column, strategy='dynamic', charset=None, n_rows_guess=128):
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
            n_rows_guess (int|None): approximate number of rows when 'n_rows' is not set

        Returns:
            list: List of strings in the column
        '''
        allowed = ['binary', 'unigram', 'fivegram', 'dynamic']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        ctx = search_alg.Context(table, column, None, None)
        n_rows = search_alg.IntExponentialBinarySearch(
            requester=self.requester,
            query_cb=self.dbms.RowsQueries.rows_count,
            upper=n_rows_guess,
            find_range=True,
        ).run(ctx)

        if strategy == 'binary':
            return collect.BinaryTextCollector(
                requester=self.requester,
                queries=self.dbms.RowsQueries,
                charset=charset,
            ).run(ctx, n_rows)
        elif strategy in ['unigram', 'fivegram']:
            ngram = 1 if strategy == 'unigram' else 5
            return collect.AdaptiveTextCollector(
                requester=self.requester,
                queries=self.dbms.RowsQueries,
                model=hakuin.Model(ngram),
                charset=charset,
            ).run(ctx, n_rows)
        else:
            return collect.DynamicTextCollector(
                requester=self.requester,
                queries=self.dbms.RowsQueries,
                charset=charset,
            ).run(ctx, n_rows)
