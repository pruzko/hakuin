import hakuin
import hakuin.search_algorithms as alg
import hakuin.collectors as coll

from hakuin.utils import CHARSET_DIGITS



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
        ctx.n_rows = alg.NumericBinarySearch(
            requester=self.requester,
            query_cb=self.dbms.q_rows_count_lt,
            lower=0,
            upper=8,
            find_lower=False,
            find_upper=True,
        ).run(ctx)

        if strategy == 'binary':
            return coll.BinaryTextCollector(
                requester=self.requester,
                dbms=self.dbms,
            ).run(ctx)
        else:
            return coll.ModelTextCollector(
                requester=self.requester,
                dbms=self.dbms,
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
        ctx.n_rows = alg.NumericBinarySearch(
            requester=self.requester,
            query_cb=self.dbms.q_rows_count_lt,
            lower=0,
            upper=8,
            find_lower=False,
            find_upper=True,
        ).run(ctx)

        if strategy == 'binary':
            return coll.BinaryTextCollector(
                requester=self.requester,
                dbms=self.dbms,
            ).run(ctx)
        else:
            return coll.ModelTextCollector(
                requester=self.requester,
                dbms=self.dbms,
                model=hakuin.get_model_columns(),
            ).run(ctx)


    def extract_schema(self, strategy='model'):
        '''Extracts schema.

        Params:
            strategy (str): 'binary' for binary search or 'model' for pre-trained
                            models with Huffman trees
        Returns:
            dict: schema
        '''
        allowed = ['binary', 'model']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        schema = {}
        for table in self.extract_table_names(strategy):
            schema[table] = self.extract_column_names(table, strategy)

        return schema


    def extract_column_data_type(self, table, column):
        '''Extracts column data type.

        Params:
            table (str): table name
            column (str): column name

        Returns:
            string: column data type
        '''
        ctx = coll.Context(table=table, column=column)

        return alg.BinarySearch(
            requester=self.requester,
            query_cb=self.dbms.q_column_data_type,
            values=self.dbms.DATA_TYPES,
        ).run(ctx)


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
                dbms=self.dbms,
                charset=charset,
            ).run(ctx)
        elif strategy in ['unigram', 'fivegram']:
            ngram = 1 if strategy == 'unigram' else 5
            return coll.AdaptiveTextCollector(
                requester=self.requester,
                dbms=self.dbms,
                model=hakuin.Model(ngram),
                charset=charset,
            ).run(ctx)
        else:
            return coll.DynamicTextCollector(
                requester=self.requester,
                dbms=self.dbms,
                charset=charset,
            ).run(ctx)


    def extract_column_int(self, table, column):
        '''Extracts integer column.

        Params:
            table (str): table name
            column (str): column name

        Returns:
            list: list of integers in the column
        '''
        ctx = coll.Context(table=table, column=column)
        return coll.IntCollector(
                requester=self.requester,
                dbms=self.dbms,
        ).run(ctx)


    def extract_column_float(self, table, column):
        '''Extracts float column.

        Params:
            table (str): table name
            column (str): column name

        Returns:
            list: list of floats in the column
        '''
        ctx = coll.Context(table=table, column=column, rows_are_ascii=True)
        res = coll.BinaryTextCollector(
            requester=self.requester,
            dbms=self.dbms,
            charset=CHARSET_DIGITS,
        ).run(ctx)
        return [float(v) if v is not None else None for v in res]
