import sqlite3

import pynamodb.exceptions
import pytest
from moto import mock_dynamodb
from pytest_lazyfixture import lazy_fixture


@pytest.mark.parametrize(
    'adapter_test_table', [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))]
)
@mock_dynamodb
def test_unversioned_save_duplicate_does_not_raise(adapter_test_table):

    if adapter_test_table.exists():
        adapter_test_table.delete_table()
    adapter_test_table.create_table()

    itm0 = adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty123")
    itm1 = adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty123")
    itm0.save()
    itm1.save()

    # query on model
    res = list(
        adapter_test_table.query(
            itm0.my_hash_key,
            adapter_test_table.my_range_key == "qwerty123",
        )
    )
    assert len(res) == 1


@pytest.mark.parametrize(
    'adapter_test_table', [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))]
)
@mock_dynamodb
def test_unversioned_save_duplicate_does_update(adapter_test_table):

    if adapter_test_table.exists():
        adapter_test_table.delete_table()
    adapter_test_table.create_table()

    itm0 = adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty123", my_payload="X")
    itm1 = adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty123", my_payload="Y")
    itm0.save()
    itm1.save()

    # query on model
    res = list(
        adapter_test_table.query(
            itm0.my_hash_key,
            adapter_test_table.my_range_key == "qwerty123",
        )
    )
    assert len(res) == 1
    assert res[0].my_payload == "Y"


@pytest.mark.parametrize(
    'adapter_test_table',
    [(lazy_fixture('sqlite_adapter_test_table_versioned')), (lazy_fixture('pynamodb_adapter_test_table_versioned'))],
)
@mock_dynamodb
def test_versioned_save_duplicate_raises(adapter_test_table):
    """This relies on pynamodb version attribute

    see https://pynamodb.readthedocs.io/en/stable/optimistic_locking.html#version-attribute
    """

    if adapter_test_table.exists():
        adapter_test_table.delete_table()
    adapter_test_table.create_table()

    with pytest.raises((pynamodb.exceptions.PutError, sqlite3.IntegrityError)):
        itm0 = adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty123", my_payload="X")
        itm1 = adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty123", my_payload="Y")
        itm0.save()
        itm1.save()  # trigger the exception

    # query on model
    res = list(
        adapter_test_table.query(
            itm0.my_hash_key,
            adapter_test_table.my_range_key == "qwerty123",
        )
    )
    assert len(res) == 1
    assert res[0].my_payload == "X"


@pytest.mark.parametrize(
    'adapter_test_table',
    [(lazy_fixture('sqlite_adapter_test_table_versioned')), (lazy_fixture('pynamodb_adapter_test_table_versioned'))],
)
@mock_dynamodb
def test_batch_save_duplicate_does_update(adapter_test_table):
    """regardless of version attribute, the last item wins in batch mode"""

    if adapter_test_table.exists():
        adapter_test_table.delete_table()
    adapter_test_table.create_table()

    with adapter_test_table.batch_write() as batch:
        itm0 = adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty123", my_payload="X")
        itm1 = adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty123", my_payload="Y")
        batch.save(itm0)
        batch.save(itm1)

    # query on model
    res = list(
        adapter_test_table.query(
            itm0.my_hash_key,
            adapter_test_table.my_range_key == "qwerty123",
        )
    )
    assert len(res) == 1
    assert res[0].my_payload == "Y"
