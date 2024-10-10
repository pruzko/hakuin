from .DBMS import DBMS
from .MSSQL import MSSQL
from .MySQL import MySQL
from .OracleDB import OracleDB
from .PSQL import PSQL
from .SQLite import SQLite



DBMS_DICT = {
    'sqlite': SQLite,
    'mysql': MySQL,
    'psql': PSQL,
    'mssql': MSSQL,
    'oracledb': OracleDB,
}