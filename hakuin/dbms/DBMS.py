import re
from abc import ABCMeta, abstractmethod



class DBMS(metaclass=ABCMeta):
    RE_NORM = re.compile(r'[ \n]+')

    DATA_TYPES = []



    @staticmethod
    def normalize(s):
        return DBMS.RE_NORM.sub(' ', s).strip()


    @abstractmethod
    def count_rows(self, ctx, n):
        raise NotImplementedError()

    @abstractmethod
    def count_tables(self, ctx, n):
        raise NotImplementedError()

    @abstractmethod
    def count_columns(self, ctx, n):
        raise NotImplementedError()

    @abstractmethod
    def meta_type(self, ctx, values):
        raise NotImplementedError()

    @abstractmethod
    def meta_is_nullable(self, ctx):
        raise NotImplementedError()

    @abstractmethod
    def meta_is_pk(self, ctx):
        raise NotImplementedError()

    @abstractmethod
    def char_rows(self, ctx, values):
        raise NotImplementedError()

    @abstractmethod
    def char_tables(self, ctx, values):
        raise NotImplementedError()

    @abstractmethod
    def char_columns(self, ctx, values):
        raise NotImplementedError()

    @abstractmethod
    def string_rows(self, ctx, values):
        raise NotImplementedError()
