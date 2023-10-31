import hakuin
import hakuin.search_algorithms as alg
import hakuin.collectors as coll



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
            list: list of extracted table names
        '''
        allowed = ['binary', 'model']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        ctx = coll.Context(rows_have_null=False)
        ctx.n_rows = alg.IntExponentialBinarySearch(
            requester=self.requester,
            query_cb=self.dbms.TablesQueries.rows_count,
            lower=0,
            upper=8,
            find_lower=False,
            find_upper=True,
        ).run(ctx)

        if strategy == 'binary':
            return coll.BinaryTextCollector(
                requester=self.requester,
                queries=self.dbms.TablesQueries,
            ).run(ctx)
        else:
            return coll.ModelTextCollector(
                requester=self.requester,
                queries=self.dbms.TablesQueries,
                model=hakuin.get_model_tables(),
            ).run(ctx)


    def extract_column_names(self, table, strategy='model'):
        '''Extracts table column names.

        Params:
            table (str): table name
            strategy (str): 'binary' for binary search or 'model' for pre-trained
                        models with Huffman trees

        Returns:
            list: list of extracted column names
        '''
        allowed = ['binary', 'model']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        ctx = coll.Context(table=table, rows_have_null=False)
        ctx.n_rows = alg.IntExponentialBinarySearch(
            requester=self.requester,
            query_cb=self.dbms.ColumnsQueries.rows_count,
            lower=0,
            upper=8,
            find_lower=False,
            find_upper=True,
        ).run(ctx)

        if strategy == 'binary':
            return coll.BinaryTextCollector(
                requester=self.requester,
                queries=self.dbms.ColumnsQueries,
            ).run(ctx)
        else:
            return coll.ModelTextCollector(
                requester=self.requester,
                queries=self.dbms.ColumnsQueries,
                model=hakuin.get_model_columns(),
            ).run(ctx)


    def extract_column_metadata(self, table, column):
        '''Extracts column metadata (data type, nullable, and primary key).

        Params:
            table (str): table name
            column (str): column name

        Returns:
            dict: column metadata
        '''
        ctx = coll.Context(table, column, None, None)

        d_type = alg.BinarySearch(
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
                metadata = self.extract_column_metadata(table, column) if metadata else None
                schema[table][column] = metadata

        return schema


    def extract_column_text(self, table, column, strategy='dynamic', charset=None):
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

        Returns:
            list: list of strings in the column
        '''
        allowed = ['binary', 'unigram', 'fivegram', 'dynamic']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        ctx = coll.Context(table=table, column=column)
        if strategy == 'binary':
            return coll.BinaryTextCollector(
                requester=self.requester,
                queries=self.dbms.TextQueries,
                charset=charset,
            ).run(ctx)
        elif strategy in ['unigram', 'fivegram']:
            ngram = 1 if strategy == 'unigram' else 5
            return coll.AdaptiveTextCollector(
                requester=self.requester,
                queries=self.dbms.TextQueries,
                model=hakuin.Model(ngram),
                charset=charset,
            ).run(ctx)
        else:
            return coll.DynamicTextCollector(
                requester=self.requester,
                queries=self.dbms.TextQueries,
                charset=charset,
            ).run(ctx)


    def extract_column_int(self, table, column):
        '''Extracts text column.

        Params:
            table (str): table name
            column (str): column name

        Returns:
            list: list of integers in the column
        '''
        ctx = coll.Context(table=table, column=column)
        return coll.IntCollector(
                requester=self.requester,
                queries=self.dbms.IntQueries,
        ).run(ctx)
