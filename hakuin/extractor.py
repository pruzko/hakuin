from hakuin import Model, get_model
from hakuin.collectors import IntContext, FloatContext, TextContext, BlobContext
from hakuin.collectors.builders import (
    IntCollectorBuilder,
    FloatCollectorBuilder,
    TextCollectorBuilder,
    MetaCollectorBuilder,
    BlobCollectorBuilder,
)
from hakuin.dbms import DBMS_DICT



class Extractor:
    '''Class for extracting DB.'''
    def __init__(
        self, requester, dbms, n_tasks=1, use_models=True, use_auto_inc=True, use_guessing=True,
        use_ternary=False,
    ):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (string|DBMS): database engine
            n_tasks (int): number of extraction tasks to run in parallel
            use_models (bool): use language models flag
            use_auto_inc (bool): use auto increment guessing flag
            use_guessing (bool): use value guessing flag
            use_ternary (bool): use ternary search flag
        '''
        self.requester = requester
        self.n_tasks = n_tasks
        self.use_models = use_models
        self.use_auto_inc = use_auto_inc
        self.use_guessing = use_guessing
        self.use_ternary = use_ternary

        if type(dbms) is str:
            assert dbms.lower() in DBMS_DICT, f'DBMS "{dbms}" is not supported.'
            self.dbms = DBMS_DICT[dbms.lower()]()
        else:
            assert issubclass(dbms, DBMS), 'The dbms object must be derived from class DBMS'
            self.dbms = dbms


    async def extract_schema_names(self):
        '''Extracts schema names.

        Params:

        Returns:
            list: list of extracted schema names
        '''
        ctx = TextContext(target='schema_names', column_has_null=False)

        builder = MetaCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            use_ternary=self.use_ternary,
            n_tasks=self.n_tasks,
        )
        if self.use_models:
            builder.add(
                collector_name='model_char_collector',
                model=get_model('schemas'),
                use_ternary=self.use_ternary,
            )

        collector = builder.build()
        return await collector.run(ctx)


    async def extract_table_names(self, schema=None):
        '''Extracts table names.

        Params:
            schema (str|None): schema name or None for default

        Returns:
            list: list of extracted table names
        '''
        ctx = TextContext(target='table_names', schema=schema, column_has_null=False)

        builder = MetaCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            use_ternary=self.use_ternary,
            n_tasks=self.n_tasks,
        )
        if self.use_models:
            builder.add(
                collector_name='model_char_collector',
                model=get_model('tables'),
                use_ternary=self.use_ternary,
            )

        collector = builder.build()
        return await collector.run(ctx)


    async def extract_column_names(self, table, schema=None):
        '''Extracts table column names.

        Params:
            table (str): table name
            schema (str|None): schema name or None for default

        Returns:
            list: list of extracted column names
        '''
        ctx = TextContext(target='column_names', schema=schema, table=table, column_has_null=False)

        builder = MetaCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            use_ternary=self.use_ternary,
            n_tasks=self.n_tasks,
        )
        if self.use_models:
            builder.add(
                collector_name='model_char_collector',
                model=get_model('columns'),
                use_ternary=self.use_ternary,
            )

        collector = builder.build()
        return await collector.run(ctx)


    async def extract_meta(self, schema=None):
        '''Extracts metadata (table and column names).

        Params:
            schema (str|None): schema name or None for default

        Returns:
            dict: table and column names
        '''
        meta = {}
        table_names = await self.extract_table_names(schema=schema)

        for table in table_names:
            meta[table] = await self.extract_column_names(table=table, schema=schema)

        return meta


    async def extract_column_type(self, table, column, schema=None):
        '''Extract column type.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None for default

        Returns:
            str|None: column type or None if unknown
        '''
        ctx = TextContext(target='column_type', schema=schema, table=table, column=column)
        type_queries = {
            'int': self.dbms.QueryColumnTypeIsInt,
            'text': self.dbms.QueryColumnTypeIsText,
            'float': self.dbms.QueryColumnTypeIsFloat,
            'blob': self.dbms.QueryColumnTypeIsBlob,
        }

        for column_type, query_cls in type_queries.items():
            if await self.requester.run(query=query_cls(dbms=self.dbms), ctx=ctx):
                return column_type

        return None


    async def extract_column(self, table, column, schema=None):
        '''Extracts column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None for default

        Returns:
            list: list of values in the column

        Raises:
            NotImplementedError: when the column type is not int/float/text/blob
        '''
        column_type = await self.extract_column_type(table=table, column=column, schema=schema)
        if not column_type:
            raise NotImplementedError(f'Unsupported column data type of "{table}.{column}".')

        extract_func = getattr(self, f'extract_column_{column_type}')
        return await extract_func(table=table, column=column, schema=schema)


    async def extract_column_int(self, table, column, schema=None):
        '''Extracts integer column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None for default

        Returns:
            list: list of integers in the column
        '''
        ctx = IntContext(target='column', schema=schema, table=table, column=column)

        builder = IntCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            use_ternary=self.use_ternary,
            n_tasks=self.n_tasks,
        )
        builder.add('binary_row_collector', use_ternary=self.use_ternary)
        if self.use_auto_inc:
            builder.add('auto_inc_row_collector')
        if self.use_guessing:
            builder.add('guessing_row_collector', use_ternary=self.use_ternary)

        collector = builder.build()
        return await collector.run(ctx)


    async def extract_column_float(self, table, column, schema=None):
        '''Extracts float column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None for default
            use_models (bool): use language models flag
            use_guessing (bool): use value guessing flag

        Returns:
            list: list of floats in the column
        '''
        ctx = FloatContext(target='column', schema=schema, table=table, column=column)

        builder = FloatCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            use_ternary=self.use_ternary,
            n_tasks=self.n_tasks,
        )
        if self.use_models:
            builder.add('model_char_collector', model=Model(1), use_ternary=self.use_ternary)
            builder.add('model_char_collector', model=Model(5), use_ternary=self.use_ternary)
        if self.use_guessing:
            builder.add('guessing_row_collector', use_ternary=self.use_ternary)

        collector = builder.build()
        return await collector.run(ctx)


    async def extract_column_text(self, table, column, schema=None, charset=None):
        '''Extracts text column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None for default
            charset (list|None): list of possible characters
            use_models (bool): use language models flag
            use_guessing (bool): use value guessing flag

        Returns:
            list: list of strings in the column
        '''
        ctx = TextContext(target='column', schema=schema, table=table, column=column)

        builder = TextCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            use_ternary=self.use_ternary,
            n_tasks=self.n_tasks,
        )
        builder.add('binary_char_collector', use_ternary=self.use_ternary)
        if self.use_models:
            builder.add('model_char_collector', model=Model(1), use_ternary=self.use_ternary)
            builder.add('model_char_collector', model=Model(5), use_ternary=self.use_ternary)
        if self.use_guessing:
            builder.add('guessing_row_collector', use_ternary=self.use_ternary)

        collector = builder.build()
        return await collector.run(ctx)


    async def extract_column_blob(self, table, column, schema=None):
        '''Extracts blob column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None for default

        Returns:
            bytes: list of bytes in the column
        '''
        ctx = BlobContext(target='column', schema=schema, table=table, column=column)

        builder = BlobCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            use_ternary=self.use_ternary,
            n_tasks=self.n_tasks,
        )
        builder.add('binary_char_collector', use_ternary=self.use_ternary)
        if self.use_models:
            builder.add('model_char_collector', model=Model(1), use_ternary=self.use_ternary)
            builder.add('model_char_collector', model=Model(5), use_ternary=self.use_ternary)
        if self.use_guessing:
            builder.add('guessing_row_collector', use_ternary=self.use_ternary)

        collector = builder.build()
        return await collector.run(ctx)
