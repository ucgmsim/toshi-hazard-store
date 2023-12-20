import pytest
from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model

from toshi_hazard_store.v2.db_adapter.sqlite import SqliteAdapter

SQLITE_ADAPTER = SqliteAdapter
NO_ADAPTER = Model


class MySqlModel(SQLITE_ADAPTER):
    class Meta:
        table_name = "MySQLITEModel"

    my_hash_key = UnicodeAttribute(hash_key=True)
    my_range_key = UnicodeAttribute(range_key=True)


class MyPynamodbModel(NO_ADAPTER):
    class Meta:
        table_name = "MyPynamodbModel"

    my_hash_key = UnicodeAttribute(hash_key=True)
    my_range_key = UnicodeAttribute(range_key=True)


@pytest.fixture(scope="module")
def sqlite_adapter_test_table():
    yield MySqlModel


@pytest.fixture(scope="module")
def pynamodb_adapter_test_table():
    yield MyPynamodbModel
