import logging
import pathlib
import sqlite3
import tempfile
from functools import partial

import pytest
from pynamodb.attributes import UnicodeAttribute, UnicodeSetAttribute, VersionAttribute
from pynamodb.models import Model
from pynamodb_attributes import FloatAttribute

import toshi_hazard_store.config
import toshi_hazard_store.db_adapter.sqlite.sqlite_adapter
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter
from toshi_hazard_store.db_adapter.sqlite.sqlite_store import safe_table_name

log = logging.getLogger(__name__)

adapter_folder = tempfile.TemporaryDirectory()


@pytest.fixture(autouse=True)
def default_session_fixture(request, monkeypatch):
    """
    :type request: _pytest.python.SubRequest
    :return:
    """
    log.info("Patching storage configuration")

    def temporary_adapter_connection(model_class, folder):
        dbpath = pathlib.Path(folder.name) / f"{safe_table_name(model_class)}.db"
        if not dbpath.parent.exists():
            raise RuntimeError(f'The sqlite storage folder "{dbpath.parent.absolute()}" was not found.')
        log.debug(f"get sqlite3 connection at {dbpath}")
        return sqlite3.connect(dbpath)

    monkeypatch.setattr(toshi_hazard_store.config, "SQLITE_ADAPTER_FOLDER", str(adapter_folder))
    monkeypatch.setattr(
        toshi_hazard_store.db_adapter.sqlite.sqlite_adapter,
        "get_connection",
        partial(temporary_adapter_connection, folder=adapter_folder),
    )


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
        # region = "us-east-1"


class MyPynamodbModel(FieldsMixin, Model):
    class Meta:
        table_name = "MyPynamodbModel"
        region = "us-east-1"


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
        region = "us-east-1"


@pytest.fixture(scope="module")
def sqlite_adapter_test_table_versioned():
    yield VersionedSqlModel


@pytest.fixture(scope="module")
def pynamodb_adapter_test_table_versioned():
    yield VersionedPynamodbModel
