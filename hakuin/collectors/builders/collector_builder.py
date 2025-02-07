from abc import ABCMeta, abstractmethod

from hakuin.collectors.column_collectors import ColumnCollector
from hakuin.collectors.row_collectors import GuessingRowCollector



class CollectorBuilder(metaclass=ABCMeta):
    '''Base builder class to construct collumn collectors.

    Attributes:
        COLUMN_COLLECTOR_CLS (Type[ColumnCollector]): column collector class
        COLLECTOR_MAP (dict): lookup map for supported row/char collector classes
    '''
    COLUMN_COLLECTOR_CLS = ColumnCollector
    COLLECTOR_MAP = {
        'guessing_row_collector': GuessingRowCollector,
    }


    def __init__(self, requester, dbms, n_tasks=1):
        '''Constructor.

        Params:
            requester (Requester): requester
            dbms (DBMS): database engine
            n_tasks (int): number of extraction tasks to run in parallel
        '''
        self.requester = requester
        self.dbms = dbms
        self.n_tasks = n_tasks
        self.collectors = {}


    def add(self, collector_name, **kwargs):
        '''Adds a row/char collector into the final column collector.

        Params:
            collector_name (str): name of the collector (must be in COLLECTOR_MAP)
            **kwargs: extra arguments passed to the collector's constructor
        '''
        self.collectors[collector_name] = self.COLLECTOR_MAP[collector_name](
            requester=self.requester,
            dbms=self.dbms,
            **kwargs,
        )


    @abstractmethod
    def build_row_collector(self):
        '''Builds the main row collector.

        Returns:
            RowCollector: row collector
        '''
        raise NotImplementedError


    def build(self):
        '''Builds the class collector.

        Returns:
            ColumnCollector: column collector
        '''
        return self.COLUMN_COLLECTOR_CLS(
            requester=self.requester,
            dbms=self.dbms,
            row_collector=self.build_row_collector(),
            guessing_row_collector=self.collectors.get('guessing_row_collector'),
            n_tasks=self.n_tasks,
        )
