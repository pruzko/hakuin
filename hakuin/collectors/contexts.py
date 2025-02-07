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
    column_has_null: bool = None


    def clone(self):
        '''Returns a copy of self.

        Returns:
            Context: copy
        '''
        return deepcopy(self)



@dataclass
class NumericContext(Context):
    '''Numeric collection state.'''
    column_is_positive: bool = None


@dataclass
class IntContext(NumericContext):
    '''Int collection state.'''
    column_type: str = 'int'


@dataclass
class FloatContext(NumericContext):
    '''Float collection state.'''
    column_type: str = 'float'


@dataclass
class StringContext(Context):
    '''String collection state.'''
    buffer: str = ''
    start_offset: int = 0


@dataclass
class TextContext(StringContext):
    '''Text collection state.'''
    column_type: str = 'text'
    column_is_ascii: bool = None
    row_is_ascii: bool = None


@dataclass
class BlobContext(StringContext):
    '''Blob collection state.'''
    column_type: str = 'blob'
    buffer: bytes = b''
