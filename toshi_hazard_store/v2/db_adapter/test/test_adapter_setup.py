# from moto import mock_dynamodb
# from nzshm_common.location.code_location import CodedLocation
import pytest
from moto import mock_dynamodb
from pynamodb.attributes import UnicodeAttribute
from pynamodb.models import Model
from pytest_lazyfixture import lazy_fixture

from toshi_hazard_store.v2.db_adapter.sqlite import SqliteAdapter

SQLITE_ADAPTER = SqliteAdapter
NO_ADAPTER = Model


class MySqlModel(SQLITE_ADAPTER):
    class Meta:
        table_name = "MySQLITEModel"

    my_hash_key = UnicodeAttribute(hash_key=True)
    my_range_key = UnicodeAttribute(range_key=True)


class MyPynamodbModel(NO_ADAPTER):
    # class AdapterMeta:
    #     adapter = PynamodbAdapter()
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


# def get_one_meta():
#     return dict(hash_key="XYZ", range_key="AMCDEF:350")


@pytest.mark.skip('')
def test_model_key_attribues(sqlite_adapter_test_table):
    from toshi_hazard_store.v2.db_adapter.sqlite.sqlite_store import get_hash_key

    assert get_hash_key(sqlite_adapter_test_table) == 'my_hash_key'


@pytest.mark.parametrize(
    'adapter_test_table', [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))]
)
@mock_dynamodb
def test_table_creation(adapter_test_table):
    adapter_test_table.create_table()
    assert adapter_test_table.exists()


@pytest.mark.parametrize(
    'adapter_test_table', [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))]
)
@mock_dynamodb
def test_table_create_drop(adapter_test_table):
    adapter_test_table.create_table()
    assert adapter_test_table.exists()
    adapter_test_table.delete_table()
    assert not adapter_test_table.exists()


@mock_dynamodb
@pytest.mark.parametrize(
    'adapter_test_table', [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))]
)
def test_table_save(adapter_test_table):
    adapter_test_table.create_table()
    # obj = MySqlModel(my_hash_key="ABD123", my_range_key="qwerty123")
    obj = adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty123")
    print(obj)
    obj.save()


@mock_dynamodb
@pytest.mark.parametrize(
    'adapter_test_table', [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))]
)
def test_table_save_and_query(adapter_test_table):
    adapter_test_table.create_table()
    adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty123").save()
    res = adapter_test_table.query(
        hash_key="ABD123", range_key_condition=adapter_test_table.my_range_key == "qwerty123"
    )

    result = list(res)
    assert len(result) == 1
    assert type(result[0]) == adapter_test_table
    assert result[0].my_hash_key == "ABD123"
    assert result[0].my_range_key == "qwerty123"


@mock_dynamodb
@pytest.mark.parametrize(
    'adapter_test_table', [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))]
)
def test_table_save_and_query_many(adapter_test_table):
    if adapter_test_table.exists():
        adapter_test_table.delete_table()
    adapter_test_table.create_table()

    for rk in range(10):
        adapter_test_table(my_hash_key="ABD123", my_range_key=f"qwerty123-{rk}").save()

    res = adapter_test_table.query(
        hash_key="ABD123",
    )

    result = list(res)
    assert len(result) == 10
    assert type(result[0]) == adapter_test_table
    assert result[0].my_hash_key == "ABD123"
    assert result[0].my_range_key == "qwerty123-0"
    assert result[9].my_range_key == "qwerty123-9"
