# dbcopy

Copy tables from a source database to a destination database, with both
databases using different SQL engines. This can be used, for example,
to copy the contents from a MySQL or MSSQL database to Postgres. While
less civilized, the opposite operation might also work :-P

## Installation

Please install `dbcopy` using the Python pip command:

```bash
pip install -U dbcopy
```

Depending on the database backend, you may need to also install a
connection adapter. Recommended adapters can be installed as extras 
with the package:

```bash
pip install -U dbcopy[postgres,mssql]
```

## Usage

You must always specify a source database connection URI and a target 
database URI:

```bash
dbcopy --drop postgresql://localhost/demo sqlite:///demo.sqlite3
```

There are additional parameters to skip individual tables or normalize 
tables and column names to the common snake_case form. For these options,
please see:

```bash
dbcopy --help
```