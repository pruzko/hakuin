# Hakuin: Injecting Brain into Blind SQL Injection

SQL Injection (SQLI) is a pervasive web attack where malicious input is used to dynamically build SQL queries in an unpredicted manner. Among many potential exploitations, the hacker may opt to exfiltrate the application database (DB). The exfiltration process is straightforward when the web application responds to injected queries with its data. In case the content is not exposed, the hacker can still deduce it using Blind SQLI (BSQLI), an inference technique based on response differences or time delays. Unfortunately, a common drawback of BSQLI is its low inference rate (one bit per request), which significantly limits the volume of data extracted.

Hakuin is a novel approach based on machine learning techniques to optimize BSQLI. Using probabilistic language models trained on millions of DB schemas, Hakuin infers data smartly. Compared to standard search solutions widely adopted in the industry, our method offers a significant performance improvement: Hakuin is about 4 times more effective.

Watch out this space for code release, soon!
