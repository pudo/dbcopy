import re
import logging
import stringcase
from normality import ascii_text, category_replace
from normality import UNICODE_CATEGORIES, WS

log = logging.getLogger(__name__)


class NameMapping(object):

    def __init__(self, table, prefix=None,
                 normalize_tables=False,
                 normalize_columns=False):
        self.table = table
        self.prefix = prefix
        self.normalize_tables = normalize_tables
        self.normalize_columns = normalize_columns

        self.name = table.name
        if normalize_tables:
            self.name = self.normalize(self.name)
        if prefix is not None:
            self.name = prefix + self.name

        self.columns = {}
        for column in table.columns:
            cname = column.name
            if self.normalize_columns:
                cname = self.normalize(column.name)
            self.columns[column.name] = cname

    def normalize(self, name):
        name = ascii_text(name)
        name = category_replace(name, UNICODE_CATEGORIES)
        if name.upper() == name:
            name = name.replace(WS, '_')
            name = name.lower()
        else:
            name = stringcase.snakecase(name)
        return re.sub('_+', '_', name)
