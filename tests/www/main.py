import os

from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import text



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_DBS = os.path.abspath(os.path.join(DIR_FILE, '..', 'dbs'))


app = Flask('Hakuin Test App')

app.config['SQLALCHEMY_BINDS'] = {
    # comment out DBMS that are not installed
    'sqlite': f'sqlite:///{os.path.join(DIR_DBS, "test_db.sqlite")}',
    'mysql': 'mysql+pymysql://hakuin:hakuin@localhost/hakuindb',
    'mssql': 'mssql+pyodbc://hakuin:hakuin@localhost/hakuindb?driver=ODBC+Driver+17+for+SQL+Server',
    'oracledb': 'oracle+cx_oracle://hakuin:hakuin@localhost/?service_name=freepdb1',
    'psql': 'postgresql+psycopg2://hakuin:hakuin@localhost/hakuindb',
}
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)



@app.route('/')
def index():
    return 'Hakuin Test App'


@app.route('/sqlite')
def sqlite():
    print(request.args['query'])
    query = f'SELECT cast(({request.args["query"]}) as bool)'

    with db.get_engine(app, bind='sqlite').connect() as conn:
        if conn.execute(text(query)).fetchone()[0]:
            return 'ok'

    return 'not found', 404


@app.route('/mysql')
def mysql():
    print(request.args['query'])
    query = f'SELECT ({request.args["query"]})'

    with db.get_engine(app, bind='mysql').connect() as conn:
        if conn.execute(text(query)).fetchone()[0]:
            return 'ok'

    return 'not found', 404


@app.route('/mssql')
def mssql():
    print(request.args['query'])
    query = f'SELECT iif(({request.args["query"]}) > 0, 1, 0)'

    with db.get_engine(app, bind='mssql').connect() as conn:
        if conn.execute(text(query)).fetchone()[0]:
            return 'ok'

    return 'not found', 404


@app.route('/oracledb')
def oracledb():
    print(request.args['query'])
    query = f'SELECT CASE WHEN ({request.args["query"]}) THEN 1 ELSE 0 END'

    with db.get_engine(app, bind='oracledb').connect() as conn:
        if conn.execute(text(query)).fetchone()[0]:
            return 'ok'

    return 'not found', 404


@app.route('/psql')
def psql():
    print(request.args['query'])
    query = f'SELECT cast(({request.args["query"]}) as bool)'

    with db.get_engine(app, bind='psql').connect() as conn:
        if conn.execute(text(query)).fetchone()[0]:
            return 'ok'

    return 'not found', 404



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
