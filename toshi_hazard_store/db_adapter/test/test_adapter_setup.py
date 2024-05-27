# from moto import mock_dynamodb
# from nzshm_common.location.coded_location import CodedLocation
import pytest
from moto import mock_dynamodb
from pytest_lazyfixture import lazy_fixture


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
    if adapter_test_table.exists():
        adapter_test_table.delete_table()
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
    if adapter_test_table.exists():
        adapter_test_table.delete_table()
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
def test_table_save_and_query_long_sort_key(adapter_test_table):
    if adapter_test_table.exists():
        adapter_test_table.delete_table()
    adapter_test_table.create_table()
    adapter_test_table(
        my_hash_key="-36.9~174.8",
        my_range_key="-36.870~174.770:150:000000:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==",
    ).save()
    res = adapter_test_table.query(
        hash_key="-36.9~174.8",
        range_key_condition=adapter_test_table.my_range_key
        == "-36.870~174.770:150:000000:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==",
    )

    result = list(res)
    assert len(result) == 1
    assert type(result[0]) == adapter_test_table
    assert result[0].my_hash_key == "-36.9~174.8"
    assert result[0].my_range_key == "-36.870~174.770:150:000000:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg=="


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

    res = adapter_test_table.query(hash_key="ABD123", range_key_condition=adapter_test_table.my_range_key >= 'qwerty')

    result = list(res)
    assert len(result) == 10
    assert type(result[0]) == adapter_test_table
    assert result[0].my_hash_key == "ABD123"
    assert result[0].my_range_key == "qwerty123-0"
    assert result[9].my_range_key == "qwerty123-9"
