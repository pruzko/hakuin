from .dbms import DBMS
from .mssql import MSSQL
from .mysql import MySQL
from .oracle import Oracle
from .postgres import Postgres
from .sqlite import SQLite



DBMS_DICT = {
    'mssql': MSSQL,
    'mysql': MySQL,
    'oracle': Oracle,
    'postgres': Postgres,
    'sqlite': SQLite,
}
