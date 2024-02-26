import json
import os
from unittest import mock

import pytest
from pynamodb.attributes import UnicodeAttribute, UnicodeSetAttribute
from pynamodb.models import Model

from toshi_hazard_store import model
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter


@pytest.fixture(autouse=True)
def setenvvar(tmp_path):
    # ref https://adamj.eu/tech/2020/10/13/how-to-mock-environment-variables-with-pytest/
    envvars = {
        "THS_SQLITE_FOLDER": str(tmp_path),
    }
    with mock.patch.dict(os.environ, envvars, clear=True):
        yield  # This is the magical bit which restore the environment after


class MySqlModel(SqliteAdapter, Model):
    class Meta:
        table_name = "MySQLITEModel"

    my_hash_key = UnicodeAttribute(hash_key=True)
    my_range_key = UnicodeAttribute(range_key=True)
    my_unicode_set = UnicodeSetAttribute()


class MyPynamodbModel(Model):
    class Meta:
        table_name = "MyPynamodbModel"

    my_hash_key = UnicodeAttribute(hash_key=True)
    my_range_key = UnicodeAttribute(range_key=True)
    my_unicode_set = UnicodeSetAttribute()


@pytest.fixture(scope="module")
def sqlite_adapter_test_table():
    yield MySqlModel


@pytest.fixture(scope="module")
def pynamodb_adapter_test_table():
    yield MyPynamodbModel
