import re
from abc import ABCMeta, abstractmethod



class Queries(metaclass=ABCMeta):
    '''Class for constructing SQL queries.'''
    _RE_NORMALIZE = re.compile(r'[ \n]+')


    @staticmethod
    def normalize(s):
        return Queries._RE_NORMALIZE.sub(' ', s).strip()


    @staticmethod
    def hex(s):
        return s.encode("utf-8").hex()



class MetaQueries(Queries):
    '''Interface for queries that infer DB metadata.'''
    @abstractmethod
    def column_data_type(self, ctx, values): raise NotImplementedError()
    @abstractmethod
    def column_is_nullable(self, ctx): raise NotImplementedError()
    @abstractmethod
    def column_is_pk(self, ctx): raise NotImplementedError()



class UniformQueries(Queries):
    '''Interface for queries that can be unified.'''
    @abstractmethod
    def rows_count(self, ctx): raise NotImplementedError()
    @abstractmethod
    def rows_are_ascii(self, ctx): raise NotImplementedError()
    @abstractmethod
    def row_is_ascii(self, ctx): raise NotImplementedError()
    @abstractmethod
    def char_is_ascii(self, ctx): raise NotImplementedError()
    @abstractmethod
    def char(self, ctx): raise NotImplementedError()
    @abstractmethod
    def char_unicode(self, ctx): raise NotImplementedError()
    @abstractmethod
    def string(self, ctx, values): raise NotImplementedError()



class DBMS(metaclass=ABCMeta):
    '''Database Management System (DBMS) interface.

    Attributes:
        DATA_TYPES (list): all data types available
        MetaQueries (MetaQueries): queries of metadata extraction
        TablesQueries (UniformQueries): queries for table names extraction
        ColumnsQueries (UniformQueries): queries for column names extraction
        RowsQueries (UniformQueries): queries for rows extraction
    '''
    _RE_ESCAPE = re.compile(r'[a-zA-Z0-9_#@]+')

    DATA_TYPES = []

    MetaQueries = None
    TablesQueries = None
    ColumnsQueries = None
    RowsQueries = None


    @staticmethod
    def escape(s):
        if DBMS._RE_ESCAPE.match(s):
            return s
        assert ']' not in s, f'Cannot escape "{s}"'
        return f'[{s}]'
