from copy import deepcopy
from dataclasses import dataclass



@dataclass
class Context:
    '''Collection state.'''
    target: str = None
    schema: str = None
    table: str = None
    column: str = None
    column_type: str = None
    cast_to: str = None
    n_rows: int = None
    row_idx: int = None
    rows_have_null: bool = None


    def clone(self):
        '''Returns a copy of self.

        Returns:
            Context: copy
        '''
        return deepcopy(self)



@dataclass
class NumericContext(Context):
    '''Int collection state.'''
    rows_are_positive: bool = None


@dataclass
class IntContext(NumericContext):
    column_type: str = 'int'


@dataclass
class FloatContext(NumericContext):
    column_type: str = 'float'


@dataclass
class StringContext(Context):
    buffer: str = ''
    start_offset: int = 0


@dataclass
class TextContext(StringContext):
    '''Text collection state.'''
    column_type: str = 'text'
    rows_are_ascii: bool = None
    row_is_ascii: bool = None


@dataclass
class BlobContext(StringContext):
    '''Blob collection state.'''
    column_type: str = 'blob'
    buffer: bytes = b''
