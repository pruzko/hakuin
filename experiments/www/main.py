import os

import sqlite3
from fastapi import FastAPI
from fastapi.responses import HTMLResponse


DIR_FILE = os.path.dirname(os.path.realpath(__file__))
DIR_ROOT = os.path.abspath(os.path.join(DIR_FILE, '..', '..'))
FILE_SDB = os.path.join(DIR_ROOT, 'experiments', 'schema_db', 'db.sqlite')
FILE_GDB = os.path.join(DIR_ROOT, 'experiments', 'generic_db', 'db.sqlite')


app = FastAPI()

counter = 0


@app.get('/')
def root(name: str):
    global counter
    counter += 1

    with sqlite3.connect(FILE_GDB) as db:
        db = db.cursor()
        query = f'SELECT * FROM users WHERE first_name = "{name}"'
        print('query: ', query)
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
        db = db.cursor()
        query = f'SELECT 1=1 and {name}'
        print('query: ', query)
        res = db.execute(query).fetchone()[0]
        if res:
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
