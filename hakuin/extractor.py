from hakuin import get_model_schemas, get_model_tables, get_model_columns
from hakuin.search_algorithms import BinarySearch
from hakuin.collectors import BlobCollector, FloatCollector, IntCollector, TextCollector
from hakuin.collectors import IntContext, FloatContext, TextContext, BlobContext
from hakuin.dbms import DBMS_DICT



class Extractor:
    '''Class for extracting DB.'''
    def __init__(self, requester, dbms, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): Requester instance used to inject queries
            dbms (string|DBMS): DBMS name or instance used to construct queries
            n_tasks (int): number of extraction tasks to run in parallel
        '''
        self.requester = requester
        self.n_tasks = n_tasks

        if type(dbms) is str:
            assert dbms.lower() in DBMS_DICT, f'DBMS "{dbms}" is not supported.'
            self.dbms = DBMS_DICT[dbms.lower()]()
        else:
            assert issubclass(dbms, DBMS), 'The dbms object must be derived from the DBMS class'
            self.dbms = dbms


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

        ctx = TextContext(target='schema_names', rows_have_null=False)
        ctx.n_rows = await BinarySearch(
            requester=self.requester,
            dbms=self.dbms,
            query_cls=self.dbms.QueryRowsCountLt,
            lower=0,
            upper=8,
            find_lower=False,
            find_upper=True,
        ).run(ctx)

        builder = TextCollector.MetaBuilder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_fivegram_char_collector(model=get_model_schemas())
        collector = builder.build()
        return await collector.run(ctx)


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

        ctx = TextContext(target='table_names', schema=schema, rows_have_null=False)
        ctx.n_rows = await BinarySearch(
            requester=self.requester,
            dbms=self.dbms,
            query_cls=self.dbms.QueryRowsCountLt,
            lower=0,
            upper=8,
            find_lower=False,
            find_upper=True,
        ).run(ctx)

        builder = TextCollector.MetaBuilder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_fivegram_char_collector(model=get_model_tables())
        collector = builder.build()
        return await collector.run(ctx)


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

        ctx = TextContext(target='column_names', schema=schema, table=table, rows_have_null=False)
        ctx.n_rows = await BinarySearch(
            requester=self.requester,
            dbms=self.dbms,
            query_cls=self.dbms.QueryRowsCountLt,
            lower=0,
            upper=8,
            find_lower=False,
            find_upper=True,
        ).run(ctx)

        builder = TextCollector.MetaBuilder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_fivegram_char_collector(model=get_model_columns())
        collector = builder.build()
        return await collector.run(ctx)


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


    async def extract_column_type(self, table, column, schema=None):
        '''TODO'''
        ctx = TextContext(target='column_type', schema=schema, table=table, column=column)

        queries = [
            ('int', self.dbms.QueryColumnTypeIsInt(dbms=self.dbms, ctx=ctx)),
            ('text', self.dbms.QueryColumnTypeIsText(dbms=self.dbms, ctx=ctx)),
            ('float', self.dbms.QueryColumnTypeIsFloat(dbms=self.dbms, ctx=ctx)),
            ('blob', self.dbms.QueryColumnTypeIsBlob(dbms=self.dbms, ctx=ctx)),
        ]

        for column_type, query in queries:
            if await self.requester.run(query):
                return column_type

        return None


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
        column_type = await self.extract_column_type(table=table, column=column, schema=schema)
        if column_type == 'int':
            return await self.extract_column_int(table=table, column=column, schema=schema)
        elif column_type == 'text':
            return await self.extract_column_text(table=table, column=column, schema=schema, strategy=text_strategy)
        elif column_type == 'float':
            return await self.extract_column_float(table=table, column=column, schema=schema)
        elif column_type == 'blob':
            return await self.extract_column_blob(table=table, column=column, schema=schema)

        raise NotImplementedError(f'Unsupported column data type of "{table}.{column}".')


    async def extract_column_int(self, table, column, schema=None):
        '''Extracts integer column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None if the target schema is the default schema

        Returns:
            list: list of integers in the column
        '''
        ctx = IntContext(target='column', schema=schema, table=table, column=column)

        builder = IntCollector.Builder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_guessing_row_collector()
        builder.add_auto_inc_row_collector()
        collector = builder.build()
        return await collector.run(ctx)


    async def extract_column_float(self, table, column, schema=None):
        '''Extracts float column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None if the target schema is the default schema

        Returns:
            list: list of floats in the column
        '''
        ctx = FloatContext(target='column', schema=schema, table=table, column=column)

        builder = FloatCollector.Builder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_guessing_row_collector()
        builder.add_list_binary_char_collector()
        builder.add_unigram_char_collector()
        builder.add_fivegram_char_collector()
        collector = builder.build()
        return await collector.run(ctx)


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

        ctx = TextContext(target='column', schema=schema, table=table, column=column)

        builder = TextCollector.Builder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_guessing_row_collector()
        builder.add_binary_char_collector()
        builder.add_unigram_char_collector()
        builder.add_fivegram_char_collector()
        collector = builder.build()
        return await collector.run(ctx)


    async def extract_column_blob(self, table, column, schema=None):
        '''Extracts blob column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None if the target schema is the default schema

        Returns:
            bytes: list of bytes in the column
        '''
        ctx = BlobContext(target='column', schema=schema, table=table, column=column)

        builder = BlobCollector.Builder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_guessing_row_collector()
        builder.add_binary_char_collector()
        builder.add_unigram_char_collector()
        builder.add_fivegram_char_collector()
        collector = builder.build()
        return await collector.run(ctx)
