from .collector_builder import CollectorBuilder

from hakuin.collectors.char_collectors import (
    TextBinaryCharCollector,
    TextListCharCollector,
    TextModelCharCollector,
    BlobBinaryCharCollector,
    BlobListCharCollector,
    BlobModelCharCollector,
)
from hakuin.collectors.column_collectors import TextColumnCollector, BlobColumnCollector
from hakuin.collectors.row_collectors import (
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


    def __init__(self, requester, dbms, use_ternary=False, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            use_ternary (bool): use ternary search flag
            n_tasks (int): number of extraction tasks to run in parallel
        '''
        super().__init__(requester, dbms, use_ternary, n_tasks)
        self.add('binary_char_collector', use_ternary=self.use_ternary)
        self.collectors['model_char_collectors'] = []


    def add(self, collector_name, **kwargs):
        '''Adds a row/char collector into the final column collector.

        Params:
            collector_name (str): name of the collector (must be in COLLECTOR_MAP)
            **kwargs: extra arguments passed to the collector's constructor
        '''
        if collector_name == 'model_char_collector':
            model_char_collector = self.COLLECTOR_MAP[collector_name](
                requester=self.requester,
                dbms=self.dbms,
                **kwargs,
            )
            self.collectors['model_char_collectors'].append(model_char_collector)
        else:
            super().add(collector_name, **kwargs)


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
            model_char_collectors=self.collectors['model_char_collectors'],
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
        'model_char_collector': TextModelCharCollector,
    }



class MetaCollectorBuilder(TextCollectorBuilder):
    '''Builder class to construct text column collectors for meta.

    Attributes:
        COLUMN_COLLECTOR_CLS (Type[ColumnCollector]): column collector class
        ROW_COLLECTOR_CLS (Type[RowCollector]): row collector class
        COLLECTOR_MAP (dict): lookup map for supported row/char collector classes
    '''
    ROW_COLLECTOR_CLS = MetaTextRowCollector



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
            'model_char_collector': BlobModelCharCollector,
        }
        ROW_COLLECTOR_CLS = BlobRowCollector