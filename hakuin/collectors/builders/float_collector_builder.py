from .collector_builder import CollectorBuilder
from .int_collector_builder import IntCollectorBuilder
from .string_collector_builders import TextCollectorBuilder

from hakuin.collectors.column_collectors import FloatColumnCollector
from hakuin.collectors.row_collectors import FloatRowCollector
from hakuin.utils import CHARSET_DIGITS



class FloatCollectorBuilder(CollectorBuilder):
    '''Builder class to construct float column collectors.

    Attributes:
        COLUMN_COLLECTOR_CLS (Type[ColumnCollector]): column collector class
        COLLECTOR_MAP (dict): lookup map for supported row/char collector classes
    '''
    COLUMN_COLLECTOR_CLS = FloatColumnCollector


    def __init__(self, requester, dbms, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            n_tasks (int): number of extraction tasks to run in parallel
        '''
        super().__init__(requester, dbms, n_tasks)
        self.int_collector_builder = IntCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        )
        self.text_collector_builder = TextCollectorBuilder(
            requester=self.requester,
            dbms=self.dbms,
            n_tasks=self.n_tasks,
        )
        self.add('binary_row_collector')
        self.add('list_char_collector', charset=CHARSET_DIGITS)


    def add(self, collector_name, **kwargs):
        '''Adds a row/char collector into the final column collector.

        Params:
            collector_name (str): name of the collector (must be in COLLECTOR_MAP)
            **kwargs: extra arguments passed to the collector's constructor
        '''
        if collector_name in self.COLLECTOR_MAP:
            super().add(collector_name, **kwargs)
        elif collector_name in self.int_collector_builder.COLLECTOR_MAP:
            self.int_collector_builder.add(collector_name, **kwargs)
        elif collector_name in self.text_collector_builder.COLLECTOR_MAP:
            self.text_collector_builder.add(collector_name, **kwargs)
        else:
            raise KeyError(f'Collector "{collector_name}" not found.')


    def build_row_collector(self):
        '''Builds the main row collector.

        Returns:
            FloatRowCollector: row collector
        '''
        return FloatRowCollector(
            requester=self.requester,
            dbms=self.dbms,
            binary_row_collector=self.int_collector_builder.build_row_collector(),
            dec_text_row_collector=self.text_collector_builder.build_row_collector(),
        )
