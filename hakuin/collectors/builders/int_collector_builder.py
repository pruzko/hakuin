from .collector_builder import CollectorBuilder

from hakuin.collectors.column import IntColumnCollector
from hakuin.collectors.row import AutoIncRowCollector, BinaryRowCollector, IntRowCollector



class IntCollectorBuilder(CollectorBuilder):
    '''Builder class to construct integer column collectors.

    Attributes:
        COLUMN_COLLECTOR_CLS (Type[ColumnCollector]): column collector class
        COLLECTOR_MAP (dict): lookup map for supported row/char collector classes
    '''
    COLUMN_COLLECTOR_CLS = IntColumnCollector
    COLLECTOR_MAP = {
        **CollectorBuilder.COLLECTOR_MAP,
        'binary_row_collector': BinaryRowCollector,
        'auto_inc_row_collector': AutoIncRowCollector,
    }


    def __init__(self, requester, dbms, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            n_tasks (int): number of extraction tasks to run in parallel
        '''
        super().__init__(requester, dbms, n_tasks)
        self.add('binary_row_collector')


    def build_row_collector(self):
        '''Builds the main row collector.

        Returns:
            IntRowCollector: row collector
        '''
        return IntRowCollector(
            requester=self.requester,
            dbms=self.dbms,
            binary_row_collector=self.collectors['binary_row_collector'],
            auto_inc_row_collector=self.collectors.get('auto_inc_row_collector'),
        )
