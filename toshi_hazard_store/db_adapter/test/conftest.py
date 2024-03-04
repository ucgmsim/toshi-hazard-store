import logging
import pathlib
import sqlite3
import tempfile
from functools import partial

import pytest

import toshi_hazard_store.config
import toshi_hazard_store.db_adapter.sqlite.sqlite_adapter
from toshi_hazard_store.db_adapter.sqlite.sqlite_store import safe_table_name

from .model_fixtures import MyPynamodbModel, MySqlModel, VersionedPynamodbModel, VersionedSqlModel

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


@pytest.fixture(scope="module")
def sqlite_adapter_test_table():
    yield MySqlModel


@pytest.fixture(scope="module")
def pynamodb_adapter_test_table():
    yield MyPynamodbModel


@pytest.fixture(scope="module")
def sqlite_adapter_test_table_versioned():
    yield VersionedSqlModel


@pytest.fixture(scope="module")
def pynamodb_adapter_test_table_versioned():
    yield VersionedPynamodbModel
