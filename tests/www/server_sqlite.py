import os

import sqlite3
from fastapi import FastAPI
from fastapi.responses import HTMLResponse



DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_DBS = os.path.abspath(os.path.join(DIR_FILE, '..', 'dbs'))
FILE_LARGE_CONTENT = os.path.join(DIR_DBS, 'large_content.sqlite')
FILE_LARGE_SCHEMA = os.path.join(DIR_DBS, 'large_schema.sqlite')
FILE_DATA_TYPES = os.path.join(DIR_DBS, 'data_types.sqlite')


app = FastAPI()

counter = 0


@app.get('/large_content')
def root(name: str):
    global counter
    counter += 1

    with sqlite3.connect(FILE_LARGE_CONTENT) as db:
        query = f'SELECT * FROM users WHERE first_name = "{name}"'
        print('query: ', query)
        
        db = db.cursor()
        users = db.execute(query).fetchall()
        if users:
            return HTMLResponse(content='Ok', status_code=200)
        else:
            return HTMLResponse(content='Not Found', status_code=404)


@app.get('/schemas')
def root(name: str):
    global counter
    counter += 1

    with sqlite3.connect(FILE_SDB) as db:
        query = f'SELECT "John" = "{name}"'
        print('query: ', query)

        db = db.cursor()
        res = db.execute(query).fetchone()[0]
        if res:
            return HTMLResponse(content='Ok', status_code=200)
        else:
            return HTMLResponse(content='Not Found', status_code=404)


@app.get('/data_types')
def root(name: str):
    global counter
    counter += 1

    with sqlite3.connect(FILE_DATA_TYPES) as db:
        query = f'SELECT "John" = "{name}"'
        print('query: ', query)
        
        db = db.cursor()
        users = db.execute(query).fetchone()[0]
        if users:
            return HTMLResponse(content='Ok', status_code=200)
        else:
            return HTMLResponse(content='Not Found', status_code=404)


@app.get('/counter')
def root():
    return HTMLResponse(content=f'counter: {counter}', status_code=200)


@app.get('/reset')
def root():
    global counter

    counter = 0
    return HTMLResponse(content=f'counter: {counter}', status_code=200)
