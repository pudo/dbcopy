import logging
from urllib.parse import urlparse

from sqlalchemy import Column, Table
from sqlalchemy import MetaData, create_engine
from sqlalchemy import types
from sqlalchemy.pool import NullPool
from sqlalchemy.dialects import mysql

log = logging.getLogger(__name__)


class Database(object):

    TYPE_MAPPINGS = {
        types.VARCHAR: types.Unicode,
        mysql.MEDIUMBLOB: types.BLOB,
        mysql.MEDIUMINT: types.BigInteger,
        mysql.BIGINT: types.BigInteger,
        mysql.MEDIUMTEXT: types.Unicode,
        mysql.TINYINT: types.Boolean
    }

    TYPE_BASES = (
        types.ARRAY,
        types.JSON,
        types.DateTime,
        types.Numeric,
        types.Float,
        types.BigInteger,
        types.Integer,
        types.Enum,
    )

    def __init__(self, uri):
        engine_kwargs = {
            'poolclass': NullPool
        }
        self.scheme = urlparse(uri).scheme.lower()
        # self.is_sqlite = 'sqlite' in self.scheme
        self.is_postgres = 'postgres' in self.scheme
        # self.is_mysql = 'mysql' in self.scheme
        # self.is_mssql = 'mssql' in self.scheme

        self.uri = uri
        self.meta = MetaData()
        self.engine = create_engine(uri, **engine_kwargs)
        self.meta.bind = self.engine
        self.meta.reflect(resolve_fks=False)

    @property
    def tables(self):
        return self.meta.sorted_tables

    def count(self, table):
        return self.engine.execute(table.count()).scalar()

    def _translate_type(self, type_):
        type_ = self.TYPE_MAPPINGS.get(type_, type_)
        for base in self.TYPE_BASES:
            if issubclass(type_, base):
                type_ = base
        if type_ == types.Enum:
            type_ = types.Unicode
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
                return (table, True)
            log.warning("Drop existing table: %s", mapping.name)
            table.drop(self.engine)
            self.meta.remove(table)
        target_table = Table(mapping.name, self.meta, *columns)
        target_table.create(self.engine)
        return (target_table, False)

    def _convert_value(self, value, table, column):
        if isinstance(column.type, types.DateTime):
            if value == '0000-00-00 00:00:00':
                value = None
        return value

    def copy(self, source_db, source_table, target_table, mapping,
             chunk_size=10000):
        conn = source_db.engine.connect()
        conn = conn.execution_options(stream_results=True)
        proxy = conn.execute(source_table.select())
        # log.info("Chunk size: %d", chunk_size)
        while True:
            rows = proxy.fetchmany(size=chunk_size)
            if not len(rows):
                break
            chunk = []
            for row in rows:
                item = {}
                for src_name, value in row.items():
                    target_name = mapping.columns.get(src_name)
                    column = target_table.columns[target_name]
                    value = self._convert_value(value, target_table, column)
                    item[target_name] = value
                chunk.append(item)
                yield item
            target_table.insert().execute(chunk)
