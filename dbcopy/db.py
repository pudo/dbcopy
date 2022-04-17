import logging
from urllib.parse import urlparse
from normality.cleaning import remove_unsafe_chars

from sqlalchemy import Column, Table
from sqlalchemy import MetaData, create_engine
from sqlalchemy import types
from sqlalchemy.pool import NullPool
from sqlalchemy.dialects import mysql, mssql

log = logging.getLogger(__name__)


class Database(object):

    TYPE_MAPPINGS = {
        types.CHAR: types.Unicode,
        types.VARCHAR: types.Unicode,
        types.Enum: types.Unicode,
        mssql.base.NTEXT: types.Unicode,
        mssql.base.NVARCHAR: types.Unicode,
        mssql.base.NCHAR: types.Unicode,
        mssql.base.VARCHAR: types.Unicode,
        mssql.base.BIT: types.Boolean,
        mssql.base.UNIQUEIDENTIFIER: types.Unicode,
        mssql.base.TIMESTAMP: types.LargeBinary,
        mssql.base.XML: types.Unicode,
        mssql.base.BINARY: types.LargeBinary,
        mssql.base.VARBINARY: types.LargeBinary,
        mssql.base.IMAGE: types.LargeBinary,
        mssql.base.SMALLMONEY: types.Numeric,
        mssql.base.SQL_VARIANT: types.LargeBinary,
        mysql.MEDIUMBLOB: types.LargeBinary,
        mysql.LONGBLOB: types.LargeBinary,
        mysql.MEDIUMINT: types.Integer,
        mysql.BIGINT: types.BigInteger,
        mysql.MEDIUMTEXT: types.Unicode,
        mysql.TINYTEXT: types.Unicode,
        mysql.LONGTEXT: types.Unicode,
        mysql.BLOB: types.LargeBinary,
        mysql.LONGBLOB: types.LargeBinary,
        types.BLOB: types.LargeBinary,
        types.VARBINARY: types.LargeBinary,
    }

    TYPE_BASES = (
        types.ARRAY,
        types.JSON,
        types.DateTime,
        types.BigInteger,
        types.Numeric,
        types.Float,
        types.Integer,
        types.Enum,
    )

    def __init__(self, uri):
        engine_kwargs = {
            'poolclass': NullPool
        }
        self.scheme = urlparse(uri).scheme.lower()
        # self.is_sqlite = 'sqlite' in self.scheme
        # self.is_postgres = 'postgres' in self.scheme
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
                break
        return self.TYPE_MAPPINGS.get(type_, type_)

    def create(self, table, mapping, drop=False):
        columns = []
        for column in table.columns:
            cname = mapping.columns.get(column.name)
            ctype = self._translate_type(type(column.type))
            # not reading nullable from source:
            columns.append(Column(cname, ctype, nullable=True))
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
        if isinstance(column.type, (types.DateTime, types.Date)):
            if value in ('0000-00-00 00:00:00', '0000-00-00'):
                value = None
        if isinstance(column.type, (types.String, types.Unicode)):
            if isinstance(value, str):
                value = remove_unsafe_chars(value)
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
