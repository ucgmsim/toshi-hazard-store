import os
from unittest import mock

import pytest
from pynamodb.attributes import UnicodeAttribute, UnicodeSetAttribute, VersionAttribute
from pynamodb.models import Model
from pynamodb_attributes import FloatAttribute

from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter


@pytest.fixture(autouse=True)
def setenvvar(tmp_path):
    # ref https://adamj.eu/tech/2020/10/13/how-to-mock-environment-variables-with-pytest/
    envvars = {
        "THS_SQLITE_FOLDER": str(tmp_path),
    }
    with mock.patch.dict(os.environ, envvars, clear=True):
        yield  # This is the magical bit which restore the environment after


class FieldsMixin:
    my_hash_key = UnicodeAttribute(hash_key=True)
    my_range_key = UnicodeAttribute(range_key=True)
    my_unicode_set = UnicodeSetAttribute()
    my_float = FloatAttribute(null=True)
    my_payload = UnicodeAttribute(null=True)


class VersionedFieldsMixin(FieldsMixin):
    version = VersionAttribute()


class MySqlModel(FieldsMixin, SqliteAdapter, Model):
    class Meta:
        table_name = "MySQLITEModel"


class MyPynamodbModel(FieldsMixin, Model):
    class Meta:
        table_name = "MyPynamodbModel"


@pytest.fixture(scope="module")
def sqlite_adapter_test_table():
    yield MySqlModel


@pytest.fixture(scope="module")
def pynamodb_adapter_test_table():
    yield MyPynamodbModel


# below are the versioned test fixtures
class VersionedSqlModel(VersionedFieldsMixin, SqliteAdapter, Model):
    class Meta:
        table_name = "VersionedSqlModel"


class VersionedPynamodbModel(VersionedFieldsMixin, Model):
    class Meta:
        table_name = "VersionedPynamodbModel"


@pytest.fixture(scope="module")
def sqlite_adapter_test_table_versioned():
    yield VersionedSqlModel


@pytest.fixture(scope="module")
def pynamodb_adapter_test_table_versioned():
    yield VersionedPynamodbModel
