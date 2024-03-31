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


    async def extract_schema_names(self, strategy='model'):
        '''Extracts schema names.

        Params:
            strategy (str): 'binary' for binary search or 'model' for pre-trained
                            models with Huffman trees

        Returns:
            list: list of extracted schema names
        '''
        allowed = ['binary', 'model']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        ctx = coll.Context(target='schema_names', rows_have_null=False)
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
                model=hakuin.get_model_schemas(),
                n_tasks=self.n_tasks,
            ).run(ctx)


    async def extract_table_names(self, schema=None, strategy='model'):
        '''Extracts table names.

        Params:
            schema (str|None): schema name or None if the target schema is the default schema
            strategy (str): 'binary' for binary search or 'model' for pre-trained
                            models with Huffman trees

        Returns:
            list: list of extracted table names
        '''
        allowed = ['binary', 'model']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        ctx = coll.Context(target='table_names', schema=schema, rows_have_null=False)
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


    async def extract_column_names(self, table, schema=None, strategy='model'):
        '''Extracts table column names.

        Params:
            table (str): table name
            schema (str|None): schema name or None if the target schema is the default schema
            strategy (str): 'binary' for binary search or 'model' for pre-trained
                        models with Huffman trees

        Returns:
            list: list of extracted column names
        '''
        allowed = ['binary', 'model']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        ctx = coll.Context(target='column_names', table=table, schema=schema, rows_have_null=False)
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


    async def extract_meta(self, schema=None, strategy='model'):
        '''Extracts metadata (table and column names).

        Params:
            schema (str|None): schema name or None if the target schema is the default schema
            strategy (str): 'binary' for binary search or 'model' for pre-trained
                            models with Huffman trees
        Returns:
            dict: table and column names
        '''
        allowed = ['binary', 'model']
        assert strategy in allowed, f'Invalid strategy: {strategy} not in {allowed}'

        meta = {}
        for table in await self.extract_table_names(schema=schema, strategy=strategy):
            meta[table] = await self.extract_column_names(table=table, schema=schema, strategy=strategy)

        return meta


    async def extract_column_data_type(self, table, column, schema=None):
        '''Extracts column data type.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None if the target schema is the default schema

        Returns:
            string: column data type
        '''
        ctx = coll.Context(table=table, column=column, schema=schema)

        return await alg.BinarySearch(
            requester=self.requester,
            query_cb=self.dbms.q_column_type_in_str_set,
            values=self.dbms.DATA_TYPES,
        ).run(ctx)


    async def extract_column(self, table, column, schema=None, text_strategy='dynamic'):
        '''Extracts column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None if the target schema is the default schema
            text_strategy (str): strategy for text columns (see extract_column_text)

        Returns:
            list: list of values in the column

        Raises:
            NotImplementedError: when the column type is not int/float/text/blob
        '''
        ctx = coll.Context(target='column_type', table=table, column=column, schema=schema)

        query = self.dbms.q_column_is_int(ctx)
        if await self.requester.request(ctx, query):
            return await self.extract_column_int(table=table, column=column, schema=schema)

        query = self.dbms.q_column_is_float(ctx)
        if await self.requester.request(ctx, query):
            return await self.extract_column_float(table=table, column=column, schema=schema)

        query = self.dbms.q_column_is_text(ctx)
        if await self.requester.request(ctx, query):
            return await self.extract_column_text(table=table, column=column, schema=schema, strategy=text_strategy)

        query = self.dbms.q_column_is_blob(ctx)
        if await self.requester.request(ctx, query):
            return await self.extract_column_blob(table=table, column=column, schema=schema)

        raise NotImplementedError(f'Unsupported column data type of "{ctx.table}.{ctx.column}".')


    async def extract_column_text(self, table, column, schema=None, strategy='dynamic', charset=None):
        '''Extracts text column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None if the target schema is the default schema
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

        ctx = coll.Context(target='column', table=table, column=column, schema=schema)
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


    async def extract_column_int(self, table, column, schema=None):
        '''Extracts integer column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None if the target schema is the default schema

        Returns:
            list: list of integers in the column
        '''
        ctx = coll.Context(target='column', table=table, column=column, schema=schema)
        return await coll.IntCollector(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        ).run(ctx)


    async def extract_column_float(self, table, column, schema=None):
        '''Extracts float column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None if the target schema is the default schema

        Returns:
            list: list of floats in the column
        '''
        ctx = coll.Context(target='column', table=table, column=column, schema=schema)
        return await coll.FloatCollector(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        ).run(ctx)


    async def extract_column_blob(self, table, column, schema=None):
        '''Extracts blob column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None if the target schema is the default schema

        Returns:
            bytes: list of bytes in the column
        '''
        ctx = coll.Context(target='column', table=table, column=column, schema=schema)
        return await coll.BlobCollector(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        ).run(ctx)