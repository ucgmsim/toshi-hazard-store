# test_model_baseis_dynamic.py

import os
from unittest import mock

import pytest
from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model
from pytest_lazyfixture import lazy_fixture

from toshi_hazard_store.v2.db_adapter.sqlite import SqliteAdapter

from toshi_hazard_store.model.openquake_models import ensure_class_bases_begin_with
from toshi_hazard_store import model


class MySqlModel:
    __metaclass__ = type

    class Meta:
        table_name = "MySQLITEModel"

    my_hash_key = UnicodeAttribute(hash_key=True)
    my_range_key = UnicodeAttribute(range_key=True)


def test_dynamic_baseclass():
    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MySqlModel'),  # `str` type differs on Python 2 vs. 3.
        base_class=Model,
    )

    instance = MySqlModel(my_hash_key='A', my_range_key='B')
    assert isinstance(instance, (MySqlModel, Model))

    ensure_class_bases_begin_with(
        namespace=globals(),  # __name__.__dict__,
        class_name=str('MySqlModel'),  # `str` type differs on Python 2 vs. 3.
        base_class=SqliteAdapter,
    )

    instance = MySqlModel(my_hash_key='A2', my_range_key='B2')
    assert isinstance(instance, (MySqlModel, Model, SqliteAdapter))


@pytest.fixture(scope="module")
def sqlite_adapter_base():
    yield SqliteAdapter


@pytest.fixture(scope="module")
def pynamodb_adapter_base():
    yield Model


def test_dynamic_baseclass_adapter_sqlite(sqlite_adapter_base):
    ensure_class_bases_begin_with(
        namespace=model.__dict__,
        class_name=str('ToshiOpenquakeMeta'),  # `str` type differs on Python 2 vs. 3.
        base_class=sqlite_adapter_base,
    )

    instance = MySqlModel(my_hash_key='A', my_range_key='B')
    assert isinstance(instance, (MySqlModel, sqlite_adapter_base))


def test_dynamic_baseclass_adapter_pynamodb(pynamodb_adapter_base):
    ensure_class_bases_begin_with(
        namespace=model.__dict__,
        class_name=str('ToshiOpenquakeMeta'),  # `str` type differs on Python 2 vs. 3.
        base_class=pynamodb_adapter_base,
    )

    instance = MySqlModel(my_hash_key='A', my_range_key='B')
    assert isinstance(instance, (MySqlModel, pynamodb_adapter_base))
