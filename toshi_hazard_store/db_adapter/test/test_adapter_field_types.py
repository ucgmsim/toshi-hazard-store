import pytest
from moto import mock_dynamodb
from pytest_lazyfixture import lazy_fixture


@mock_dynamodb
@pytest.mark.parametrize(
    'adapter_test_table', [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))]
)
@pytest.mark.parametrize(
    'payload, expected',
    [
        (["PGA"], {'PGA'}),
        ({"PGA"}, {'PGA'}),
        (None, None),
        (["PGA", "ABC"], {'PGA', 'ABC'}),
    ],
)
def test_table_save_and_query_unicode_set_A(adapter_test_table, payload, expected):
    if adapter_test_table.exists():
        adapter_test_table.delete_table()
    adapter_test_table.create_table()
    adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty123", my_unicode_set=payload).save()
    res = adapter_test_table.query(
        hash_key="ABD123", range_key_condition=adapter_test_table.my_range_key == "qwerty123"
    )

    result = list(res)
    assert len(result) == 1
    assert type(result[0]) == adapter_test_table
    assert result[0].my_hash_key == "ABD123"
    assert result[0].my_range_key == "qwerty123"
    assert result[0].my_unicode_set == expected
