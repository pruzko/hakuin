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
from hakuin.search_algorithms import BinarySearch



class Extractor:
    '''Class for extracting DB.'''
    def __init__(self, requester, dbms, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (string|DBMS): database engine
            n_tasks (int): number of extraction tasks to run in parallel
        '''
        self.requester = requester
        self.n_tasks = n_tasks

        if type(dbms) is str:
            assert dbms.lower() in DBMS_DICT, f'DBMS "{dbms}" is not supported.'
            self.dbms = DBMS_DICT[dbms.lower()]()
        else:
            assert issubclass(dbms, DBMS), 'The dbms object must be derived from class DBMS'
            self.dbms = dbms


    async def extract_schema_names(self, use_models=True):
        '''Extracts schema names.

        Params:
            use_models (bool): use language models flag

        Returns:
            list: list of extracted schema names
        '''
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

        builder = MetaCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        )
        if use_models:
            builder.add('fivegram_char_collector', model=get_model('schemas'))

        collector = builder.build()
        return await collector.run(ctx)


    async def extract_table_names(self, schema=None, use_models=True):
        '''Extracts table names.

        Params:
            schema (str|None): schema name or None for default
            use_models (bool): use language models flag

        Returns:
            list: list of extracted table names
        '''
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

        builder = MetaCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        )
        if use_models:
            builder.add('fivegram_char_collector', model=get_model('tables'))

        collector = builder.build()
        return await collector.run(ctx)


    async def extract_column_names(self, table, schema=None, use_models=True):
        '''Extracts table column names.

        Params:
            table (str): table name
            schema (str|None): schema name or None for default
            use_models (bool): use language models flag

        Returns:
            list: list of extracted column names
        '''
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

        builder = MetaCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        )
        if use_models:
            builder.add('fivegram_char_collector', model=get_model('columns'))

        collector = builder.build()
        return await collector.run(ctx)


    async def extract_meta(self, schema=None, use_models=True):
        '''Extracts metadata (table and column names).

        Params:
            schema (str|None): schema name or None for default
            use_models (bool): use language models flag

        Returns:
            dict: table and column names
        '''
        meta = {}
        table_names = await self.extract_table_names(schema=schema, use_models=use_models)

        for table in table_names:
            meta[table] = await self.extract_column_names(
                table=table,
                schema=schema,
                use_models=use_models,
            )

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


    async def extract_column(
            self, table, column, schema=None, use_models=True, use_auto_inc=True, use_guessing=True
        ):
        '''Extracts column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None for default
            use_models (bool): use language models flag
            use_auto_inc (bool): use auto increment guessing flag
            use_guessing (bool): use value guessing flag

        Returns:
            list: list of values in the column

        Raises:
            NotImplementedError: when the column type is not int/float/text/blob
        '''
        column_type = await self.extract_column_type(table=table, column=column, schema=schema)

        if column_type == 'int':
            return await self.extract_column_int(
                table=table,
                column=column,
                schema=schema,
                use_auto_inc=use_auto_inc,
                use_guessing=use_guessing,
            )
        elif column_type == 'float':
            return await self.extract_column_float(
                table=table,
                column=column,
                schema=schema,
                use_models=use_models,
                use_guessing=use_guessing,
            )
        elif column_type == 'text':
            return await self.extract_column_text(
                table=table,
                column=column,
                schema=schema,
                use_models=use_models,
                use_guessing=use_guessing,
            )
        elif column_type == 'blob':
            return await self.extract_column_blob(
                table=table,
                column=column,
                schema=schema,
                use_models=use_models,
                use_guessing=use_guessing,
            )

        raise NotImplementedError(f'Unsupported column data type of "{table}.{column}".')


    async def extract_column_int(
            self, table, column, schema=None, use_auto_inc=True, use_guessing=True
        ):
        '''Extracts integer column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None for default
            use_auto_inc (bool): use auto increment guessing flag
            use_guessing (bool): use value guessing flag

        Returns:
            list: list of integers in the column
        '''
        ctx = IntContext(target='column', schema=schema, table=table, column=column)

        builder = IntCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        )
        if use_auto_inc:
            builder.add('auto_inc_row_collector')
        if use_guessing:
            builder.add('guessing_row_collector')

        collector = builder.build()
        return await collector.run(ctx)


    async def extract_column_float(
            self, table, column, schema=None, use_models=True, use_guessing=True
        ):
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
            n_tasks=self.n_tasks,
        )
        if use_models:
            builder.add('unigram_char_collector', model=Model(1))
            builder.add('fivegram_char_collector', model=Model(5))
        if use_guessing:
            builder.add('guessing_row_collector')

        collector = builder.build()
        return await collector.run(ctx)


    async def extract_column_text(
            self, table, column, schema=None, charset=None, use_models=True, use_guessing=True
        ):
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
            n_tasks=self.n_tasks,
        )
        builder.add('binary_char_collector')
        if use_models:
            builder.add('unigram_char_collector', model=Model(1))
            builder.add('fivegram_char_collector', model=Model(5))
        if use_guessing:
            builder.add('guessing_row_collector')

        collector = builder.build()
        return await collector.run(ctx)


    async def extract_column_blob(
            self, table, column, schema=None, use_models=True, use_guessing=True
        ):
        '''Extracts blob column.

        Params:
            table (str): table name
            column (str): column name
            schema (str|None): schema name or None for default
            use_models (bool): use language models flag
            use_guessing (bool): use value guessing flag

        Returns:
            bytes: list of bytes in the column
        '''
        ctx = BlobContext(target='column', schema=schema, table=table, column=column)

        builder = BlobCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        )
        builder.add('binary_char_collector')
        if use_models:
            builder.add('unigram_char_collector', model=Model(1))
            builder.add('fivegram_char_collector', model=Model(5))
        if use_guessing:
            builder.add('guessing_row_collector')

        collector = builder.build()
        return await collector.run(ctx)
