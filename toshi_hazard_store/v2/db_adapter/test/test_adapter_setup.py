# from moto import mock_dynamodb
# from nzshm_common.location.code_location import CodedLocation
import pytest
from pynamodb.attributes import UnicodeAttribute

from toshi_hazard_store.v2.db_adapter import ModelAdapterMixin, sqlite_adapter

MYADAPTER = sqlite_adapter.SqliteAdapter()


class MyAdapterTable(ModelAdapterMixin):
    class Meta:
        table_name = "MyFkAdapterTable"

    class AdapterMeta:
        adapter = MYADAPTER

    my_hash_key = UnicodeAttribute(hash_key=True)
    my_range_key = UnicodeAttribute(range_key=True)


@pytest.fixture
def sqlite_adapter_test_table():
    yield MyAdapterTable


def get_one_meta():
    return dict(hash_key="XYZ", range_key="AMCDEF:350")


def test_model_key_attribues(sqlite_adapter_test_table):
    from toshi_hazard_store.model.caching.cache_store import get_hash_key

    assert get_hash_key(sqlite_adapter_test_table) == 'my_hash_key'


def test_table_creation(sqlite_adapter_test_table):
    sqlite_adapter_test_table.create_table()
    assert sqlite_adapter_test_table.exists()


def test_table_create_drop(sqlite_adapter_test_table):
    sqlite_adapter_test_table.create_table()
    assert sqlite_adapter_test_table.exists()
    sqlite_adapter_test_table.delete_table()
    assert not sqlite_adapter_test_table.exists()
