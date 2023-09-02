import mysql.connector
from fastapi import FastAPI
from fastapi.responses import HTMLResponse



app = FastAPI()

counter = 0


@app.get('/large_content')
def root(name: str):
    global counter
    counter += 1

    with mysql.connector.connect(host='localhost', user='test', password='password', database='mydb') as db:
        db = db.cursor()
        query = f'SELECT * FROM users WHERE name = "{name}"'
        print('query: ', query)
        db.execute(query)
        users = db.fetchall()
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
