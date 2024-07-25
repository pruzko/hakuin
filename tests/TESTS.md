A more proper instructions will be provided in the future.

Testing hakuin requires setting up a vulnerable web app:
1) install sqlite, mssql, mysqp, and psql on a VM
2) create user named "hakuin" with password "hakuin" and a create a DB named "hakuindb" for every DBMS
3) clone the hakuin repo to the VM
4) create the DBs with: `python tests/www/create_databases.py`
5) install drivers and python packages necessary for `tests/www/main.py`
6) start the server: `python tests/www/main.py`

Once the app is running, you can run the regression tests (preferably from the host machine):
1) run `python tests/run_tests.py <VM_IP> <APP_PORT>`
