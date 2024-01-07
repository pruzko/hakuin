import hakuin
import hakuin.search_algorithms as alg
import hakuin.collectors as coll



class Extractor:
    '''Class for extracting DB.'''
    def __init__(self, requester, dbms, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): Requester instance used to inject queries
            dbms (DBMS): DBMS instance used to construct queries
            n_tasks (int): number of extraction tasks to run in parallel
        '''
        self.requester = requester
        self.dbms = dbms
        self.n_tasks = n_tasks


    async def extract_table_names(self, strategy='model'):
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
        ctx.n_rows = await alg.NumericBinarySearch(
            requester=self.requester,
            query_cb=self.dbms.q_rows_count_lt,
            lower=0,
            upper=8,
            find_lower=False,
            find_upper=True,
        ).run(ctx)

        if strategy == 'binary':
            return await coll.BinaryTextCollector(
                requester=self.requester,
                dbms=self.dbms,
                n_tasks=self.n_tasks,
            ).run(ctx)
        else:
            return await coll.ModelTextCollector(
                requester=self.requester,
                dbms=self.dbms,
                model=hakuin.get_model_tables(),
                n_tasks=self.n_tasks,
            ).run(ctx)


    async def extract_column_names(self, table, strategy='model'):
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
        ctx.n_rows = await alg.NumericBinarySearch(
            requester=self.requester,
            query_cb=self.dbms.q_rows_count_lt,
            lower=0,
            upper=8,
            find_lower=False,
            find_upper=True,
        ).run(ctx)

        if strategy == 'binary':
            return await coll.BinaryTextCollector(
                requester=self.requester,
                dbms=self.dbms,
                n_tasks=self.n_tasks,
            ).run(ctx)
        else:
            return await coll.ModelTextCollector(
                requester=self.requester,
                dbms=self.dbms,
                model=hakuin.get_model_columns(),
                n_tasks=self.n_tasks,
            ).run(ctx)


    async def extract_schema(self, strategy='model'):
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
        for table in await self.extract_table_names(strategy):
            schema[table] = await self.extract_column_names(table, strategy)

        return schema


    async def extract_column_data_type(self, table, column):
        '''Extracts column data type.

        Params:
            table (str): table name
            column (str): column name

        Returns:
            string: column data type
        '''
        ctx = coll.Context(table=table, column=column)

        return await alg.BinarySearch(
            requester=self.requester,
            query_cb=self.dbms.q_column_type_in_str_set,
            values=self.dbms.DATA_TYPES,
        ).run(ctx)


    async def extract_column(self, table, column, text_strategy='dynamic'):
        '''Extracts column.

        Params:
            table (str): table name
            column (str): column name
            text_strategy (str): strategy for text columns (see extract_column_text)

        Returns:
            list: list of values in the column

        Raises:
            NotImplementedError: when the column type is not int/float/text/blob
        '''
        ctx = coll.Context(table=table, column=column)

        query = self.dbms.q_column_is_int(ctx)
        if await self.requester.request(ctx, query):
            return await self.extract_column_int(table, column)

        query = self.dbms.q_column_is_float(ctx)
        if await self.requester.request(ctx, query):
            return await self.extract_column_float(table, column)

        query = self.dbms.q_column_is_text(ctx)
        if await self.requester.request(ctx, query):
            return await self.extract_column_text(table, column, strategy=text_strategy)

        query = self.dbms.q_column_is_blob(ctx)
        if await self.requester.request(ctx, query):
            return await self.extract_column_blob(table, column)

        raise NotImplementedError(f'Unsupported column data type of "{ctx.table}.{ctx.column}".')


    async def extract_column_text(self, table, column, strategy='dynamic', charset=None):
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
            return await coll.BinaryTextCollector(
                requester=self.requester,
                dbms=self.dbms,
                charset=charset,
                n_tasks=self.n_tasks,
            ).run(ctx)
        elif strategy in ['unigram', 'fivegram']:
            ngram = 1 if strategy == 'unigram' else 5
            return await coll.AdaptiveTextCollector(
                requester=self.requester,
                dbms=self.dbms,
                model=hakuin.Model(ngram),
                charset=charset,
                n_tasks=self.n_tasks,
            ).run(ctx)
        else:
            return await coll.DynamicTextCollector(
                requester=self.requester,
                dbms=self.dbms,
                charset=charset,
                n_tasks=self.n_tasks,
            ).run(ctx)


    async def extract_column_int(self, table, column):
        '''Extracts integer column.

        Params:
            table (str): table name
            column (str): column name

        Returns:
            list: list of integers in the column
        '''
        ctx = coll.Context(table=table, column=column)
        return await coll.IntCollector(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        ).run(ctx)


    async def extract_column_float(self, table, column):
        '''Extracts float column.

        Params:
            table (str): table name
            column (str): column name

        Returns:
            list: list of floats in the column
        '''
        ctx = coll.Context(table=table, column=column)
        return await coll.FloatCollector(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        ).run(ctx)


    async def extract_column_blob(self, table, column):
        '''Extracts blob column.

        Params:
            table (str): table name
            column (str): column name

        Returns:
            bytes: list of bytes in the column
        '''
        ctx = coll.Context(table=table, column=column)
        return await coll.BlobCollector(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        ).run(ctx)