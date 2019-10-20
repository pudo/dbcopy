import click
import logging

from dbcopy.db import Database
from dbcopy.util import NameMapping

log = logging.getLogger('dbcopy')


@click.command()
@click.argument('source_uri', nargs=1, envvar='DBCOPY_SOURCE_URI')
@click.argument('target_uri', nargs=1, envvar='DBCOPY_TARGET_URI')
# @click.option('-o', '--outfile', type=click.File('w'), default='-')  # noqa
# @click.option('-f', '--foreign-id', help="foreign_id of the collection")
def dbcopy(source_uri, target_uri):
    """Copy tables from a source database to a target database."""
    logging.basicConfig(level=logging.DEBUG)
    # logging.getLogger('requests').setLevel(logging.WARNING)
    source_db = Database(source_uri)
    target_db = Database(target_uri)
    for table in source_db.tables:
        mapping = NameMapping(table)
        if len(mapping.columns) != len(table.columns):
            log.error("Column name collision: %r", mapping.columns)
            return

        count = source_db.count(table)
        log.info(" %s -> %s (%s columns, %s rows)",
                 table.name,
                 mapping.name,
                 len(mapping.columns),
                 count)
        target_table = target_db.create(table, mapping, drop=True)
        target_db.copy(source_db, table, target_table, mapping)
        # print(table, mapping.name, type(table), count)
        # print(mapping.name, mapping.columns)


if __name__ == "__main__":
    dbcopy()
