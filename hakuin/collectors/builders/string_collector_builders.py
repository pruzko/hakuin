from .collector_builder import CollectorBuilder

from hakuin.collectors.char import (
    TextBinaryCharCollector,
    TextListCharCollector,
    TextModelCharCollector,
    BlobBinaryCharCollector,
    BlobListCharCollector,
    BlobModelCharCollector,
)
from hakuin.collectors.column import TextColumnCollector, BlobColumnCollector
from hakuin.collectors.row import (
    StringRowCollector,
    TextRowCollector,
    MetaTextRowCollector,
    BlobRowCollector,
)



class StringCollectorBuilder(CollectorBuilder):
    '''Builder class to construct string column collectors.

    Attributes:
        COLUMN_COLLECTOR_CLS (Type[ColumnCollector]): column collector class
        ROW_COLLECTOR_CLS (Type[RowCollector]): row collector class
        COLLECTOR_MAP (dict): lookup map for supported row/char collector classes
    '''
    ROW_COLLECTOR_CLS = StringRowCollector


    def __init__(self, requester, dbms, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            n_tasks (int): number of extraction tasks to run in parallel
        '''
        super().__init__(requester, dbms, n_tasks)
        self.add('binary_char_collector')


    def build_row_collector(self):
        '''Builds the main row collector.

        Returns:
            StringRowCollector: row collector
        '''
        binary_char_collector = self.collectors['binary_char_collector']
        list_char_collector = self.collectors.get('list_char_collector')

        return self.ROW_COLLECTOR_CLS(
            requester=self.requester,
            dbms=self.dbms,
            binary_char_collector=list_char_collector or binary_char_collector,
            unigram_char_collector=self.collectors.get('unigram_char_collector'),
            fivegram_char_collector=self.collectors.get('fivegram_char_collector'),
        )



class TextCollectorBuilder(StringCollectorBuilder):
    '''Builder class to construct text column collectors.

    Attributes:
        COLUMN_COLLECTOR_CLS (Type[ColumnCollector]): column collector class
        ROW_COLLECTOR_CLS (Type[RowCollector]): row collector class
        COLLECTOR_MAP (dict): lookup map for supported row/char collector classes
    '''
    COLUMN_COLLECTOR_CLS = TextColumnCollector
    ROW_COLLECTOR_CLS = TextRowCollector
    COLLECTOR_MAP = {
        **StringCollectorBuilder.COLLECTOR_MAP,
        'binary_char_collector': TextBinaryCharCollector,
        'list_char_collector': TextListCharCollector,
        'unigram_char_collector': TextModelCharCollector,
        'fivegram_char_collector': TextModelCharCollector,
    }



class MetaCollectorBuilder(TextCollectorBuilder):
    '''Builder class to construct text column collectors for meta.

    Attributes:
        COLUMN_COLLECTOR_CLS (Type[ColumnCollector]): column collector class
        ROW_COLLECTOR_CLS (Type[RowCollector]): row collector class
        COLLECTOR_MAP (dict): lookup map for supported row/char collector classes
    '''
    ROW_COLLECTOR_CLS = MetaTextRowCollector


    def build_row_collector(self):
        '''Builds the main row collector.

        Returns:
            MetaRowCollector: row collector
        '''
        binary_char_collector = self.collectors['binary_char_collector']
        list_char_collector = self.collectors.get('list_char_collector')

        return self.ROW_COLLECTOR_CLS(
            requester=self.requester,
            dbms=self.dbms,
            binary_char_collector=list_char_collector or binary_char_collector,
            fivegram_char_collector=self.collectors.get('fivegram_char_collector'),
        )



class BlobCollectorBuilder(StringCollectorBuilder):
        '''Builder class to construct blob column collectors.

        Attributes:
            COLUMN_COLLECTOR_CLS (Type[ColumnCollector]): column collector class
            ROW_COLLECTOR_CLS (Type[RowCollector]): row collector class
            COLLECTOR_MAP (dict): lookup map for supported row/char collector classes
        '''
        COLUMN_COLLECTOR_CLS = BlobColumnCollector
        COLLECTOR_MAP = {
            **StringCollectorBuilder.COLLECTOR_MAP,
            'binary_char_collector': BlobBinaryCharCollector,
            'list_char_collector': BlobListCharCollector,
            'unigram_char_collector': BlobModelCharCollector,
            'fivegram_char_collector': BlobModelCharCollector,
        }
        ROW_COLLECTOR_CLS = BlobRowCollector