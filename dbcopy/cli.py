import click
import logging

log = logging.getLogger(__name__)


@click.command()
@click.option('-o', '--outfile', type=click.File('w'), default='-')  # noqa
@click.option('-f', '--foreign-id', help="foreign_id of the collection")
def dbcopy():
    """Load entities from the server and print them to stdout."""
    logging.basicConfig(level=logging.DEBUG)
    # logging.getLogger('requests').setLevel(logging.WARNING)
    print("FOO")


if __name__ == "__main__":
    dbcopy()
