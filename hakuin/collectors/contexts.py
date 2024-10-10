from dataclasses import dataclass



@dataclass
class Context:
    '''Collection state.'''
    target: str = None
    schema: str = None
    table: str = None
    column: str = None
    n_rows: int = None
    row_idx: int = None
    rows_have_null: bool = None
    row_is_null: bool = None


@dataclass
class IntContext(Context):
    '''Int collection state.'''
    rows_are_positive: bool = None


@dataclass
class TextContext(Context):
    '''Blob collection state.'''
    buffer: str = ''
    start_offset: int = 0
    rows_are_ascii: bool = None
    row_is_ascii: bool = None


@dataclass
class BlobContext(Context):
    '''Blob collection state.'''
    buffer: bytes = b''
    start_offset: int = 0
