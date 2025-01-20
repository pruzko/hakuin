# DELETE after PR accepted
from sqlglot import exp
from sqlglot.dialects import Oracle as OracleDialect
from sqlglot.dialects.dialect import rename_func

OracleDialect.Generator.TRANSFORMS.update({
    exp.LogicalOr: rename_func("MAX"),
    exp.LogicalAnd: rename_func("MIN"),
})



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