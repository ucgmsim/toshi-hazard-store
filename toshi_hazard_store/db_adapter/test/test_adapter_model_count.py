import pytest
from moto import mock_dynamodb
from pytest_lazyfixture import lazy_fixture


@pytest.mark.parametrize(
    'adapter_test_table', [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))]
)
@mock_dynamodb
def test_table_count(adapter_test_table):
    if adapter_test_table.exists():
        adapter_test_table.delete_table()
    adapter_test_table.create_table()

    with adapter_test_table.batch_write() as batch:
        for rk in range(26):

            itm = adapter_test_table(my_hash_key="ABD123", my_range_key=f"qwerty123-{rk:{0}3}", my_payload="F")
            batch.save(itm)

    result = adapter_test_table.count(
        hash_key="ABD123",
        range_key_condition=adapter_test_table.my_range_key >= 'qwerty123-016',
        filter_condition=(adapter_test_table.my_payload == "F"),
    )
    assert result == 10
