from collections import OrderedDict

from unittest import TestCase
from mock import patch

from sqlquery._querybuilder import QueryBuilder


def _ordered_dict_from_dict(unordered_dict):
    ordered = OrderedDict()
    for key in sorted(unordered_dict.keys()):
        ordered[key] = unordered_dict[key]

    return ordered


def _ordered_copy(self, **kwargs):
    if 'update' in kwargs:
        kwargs['update'] = _ordered_dict_from_dict(kwargs['update'])
    if 'insert' in kwargs:
        kwargs['insert'] = [
            _ordered_dict_from_dict(row)
            for row in kwargs['insert']
        ]

    for list_entry in ('select', 'having', 'group_by', 'where'):
        if list_entry in kwargs:
            if isinstance(kwargs[list_entry], tuple):
                kwargs[list_entry] = tuple(sorted(kwargs[list_entry]))
            elif isinstance(kwargs[list_entry], list):
                kwargs[list_entry] = sorted(kwargs[list_entry])
            else:
                kwargs[list_entry] = kwargs[list_entry]

    return QueryBuilder(self._query_data._replace(**kwargs))


class BaseTestCase(TestCase):
    def setUp(self):
        self._patch_start_query_builder_replace()
        self.builder = QueryBuilder()

    def tearDown(self):
        self.__patched.__exit__()

    def _patch_start_query_builder_replace(self):
        self.__patched = patch.object(QueryBuilder, '_replace', _ordered_copy)
        self.__patched.__enter__()
