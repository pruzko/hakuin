import hakuin
import hakuin.collectors as coll
import hakuin.collectors.char_collectors as char_coll
import hakuin.collectors.row_collectors as row_coll
from hakuin.collectors import IntCollectorBuilder, FloatCollectorBuilder, StringCollectorBuilder
import hakuin.search_algorithms as alg
from hakuin.dbms import DBMS_DICT
from hakuin.utils import CHARSET_DIGITS



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
            assert dbms.lower() in DBMS_DICT, f'DBMS "{dbms}" is not supported, choose one of {list(DBMS_DICT.keys())} instead.'
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

        ctx = coll.TextContext(target='schema_names', rows_have_null=False)
        ctx.n_rows = await alg.BinarySearch(
            requester=self.requester,
            dbms=self.dbms,
            query_cls=self.dbms.QueryRowsCountLt,
            lower=0,
            upper=8,
            find_lower=False,
            find_upper=True,
        ).run(ctx)

        builder = StringCollectorBuilder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_fivegram(model=hakuin.get_model_schemas())
        collector = builder.build(use_meta=True)
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

        ctx = coll.TextContext(target='table_names', schema=schema, rows_have_null=False)
        ctx.n_rows = await alg.BinarySearch(
            requester=self.requester,
            dbms=self.dbms,
            query_cls=self.dbms.QueryRowsCountLt,
            lower=0,
            upper=8,
            find_lower=False,
            find_upper=True,
        ).run(ctx)

        builder = StringCollectorBuilder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_fivegram(model=hakuin.get_model_tables())
        collector = builder.build(use_meta=True)
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

        ctx = coll.TextContext(target='column_names', schema=schema, table=table, rows_have_null=False)
        ctx.n_rows = await alg.BinarySearch(
            requester=self.requester,
            dbms=self.dbms,
            query_cls=self.dbms.QueryRowsCountLt,
            lower=0,
            upper=8,
            find_lower=False,
            find_upper=True,
        ).run(ctx)

        builder = StringCollectorBuilder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_fivegram(model=hakuin.get_model_columns())
        collector = builder.build(use_meta=True)
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

        query = self.dbms.QueryColumnTypeIsInt(dbms=self.dbms, ctx=ctx)
        if await self.requester.run(query):
            return await self.extract_column_int(table=table, column=column, schema=schema)

        query = self.dbms.QueryColumnTypeIsText(dbms=self.dbms, ctx=ctx)
        if await self.requester.run(query):
            return await self.extract_column_text(table=table, column=column, schema=schema, strategy=text_strategy)

        query = self.dbms.QueryColumnTypeIsFloat(dbms=self.dbms, ctx=ctx)
        if await self.requester.run(query):
            return await self.extract_column_float(table=table, column=column, schema=schema)

        query = self.dbms.QueryColumnTypeIsBlob(dbms=self.dbms, ctx=ctx)
        if await self.requester.run(query):
            return await self.extract_column_blob(table=table, column=column, schema=schema)

        raise NotImplementedError(f'Unsupported column data type of "{ctx.table}.{ctx.column}".')


    async def extract_column_int(self, table, column, schema=None):
        '''Extracts integer column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None if the target schema is the default schema

        Returns:
            list: list of integers in the column
        '''
        ctx = coll.IntContext(target='column', schema=schema, table=table, column=column)

        builder = IntCollectorBuilder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_guessing()
        builder.add_auto_inc()
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
        ctx = coll.IntContext(target='column', schema=schema, table=table, column=column)

        builder = FloatCollectorBuilder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_guessing()
        builder.add_unigram()
        builder.add_fivegram()
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

        ctx = coll.TextContext(target='column', table=table, column=column, schema=schema)

        builder = StringCollectorBuilder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_guessing()
        builder.add_binary()
        builder.add_unigram()
        builder.add_fivegram()
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
        ctx = coll.BlobContext(target='column', schema=schema, table=table, column=column)

        builder = StringCollectorBuilder(requester=self.requester, dbms=self.dbms, n_tasks=self.n_tasks)
        builder.add_guessing()
        builder.add_binary()
        builder.add_unigram()
        builder.add_fivegram()
        collector = builder.build()
        return await collector.run(ctx)
