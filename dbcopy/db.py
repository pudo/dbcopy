import logging
from urllib.parse import urlparse

from sqlalchemy import Column, Table
from sqlalchemy import MetaData, create_engine
from sqlalchemy import types
from sqlalchemy.pool import NullPool

log = logging.getLogger(__name__)


class Database(object):

    TYPE_MAPPINGS = {
        types.Enum: types.Unicode,
        types.VARCHAR: types.Unicode,
    }

    TYPE_BASES = (
        types.ARRAY,
        types.JSON,
        types.Float,
        types.BigInteger,
        types.Integer,
    )

    def __init__(self, uri):
        engine_kwargs = {
            'poolclass': NullPool
        }
        scheme = urlparse(uri).scheme.lower()
        self.is_sqlite = 'sqlite' in scheme
        self.is_postgres = 'postgres' in scheme
        self.is_mysql = 'mysql' in scheme
        self.is_mssql = 'mssql' in scheme

        self.uri = uri
        self.meta = MetaData()
        self.engine = create_engine(uri, **engine_kwargs)
        self.meta.bind = self.engine
        self.meta.reflect()

    @property
    def tables(self):
        return reversed(self.meta.sorted_tables)

    def count(self, table):
        return self.engine.execute(table.count()).scalar()

    def _translate_type(self, type_):
        type_ = self.TYPE_MAPPINGS.get(type_, type_)
        for base in self.TYPE_BASES:
            if issubclass(type_, base):
                type_ = base
        return type_

    def create(self, table, mapping, drop=False):
        columns = []
        for column in table.columns:
            cname = mapping.columns.get(column.name)
            ctype = self._translate_type(type(column.type))
            columns.append(Column(cname, ctype, nullable=column.nullable))
        if mapping.name in self.meta.tables:
            table = self.meta.tables[mapping.name]
            if not drop:
                return table
            table.drop(self.engine)
            self.meta.remove(table)
        target_table = Table(mapping.name, self.meta, *columns)
        target_table.create(self.engine)
        return target_table

    def copy(self, source_db, source_table, target_table, mapping,
             chunk_size=10000):
        conn = source_db.engine.connect()
        conn = conn.execution_options(stream_results=True)
        proxy = conn.execute(source_table.select())
        total = 0
        while True:
            rows = proxy.fetchmany(size=chunk_size)
            total += len(rows)
            if not len(rows):
                break
            chunk = []
            for row in rows:
                item = {}
                for src_name, value in row.items():
                    target_name = mapping.columns.get(src_name)
                    item[target_name] = value
                chunk.append(item)
                yield item
            target_table.insert().execute(chunk)
