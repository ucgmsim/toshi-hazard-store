from datetime import datetime, timezone

import pytest
from moto import mock_dynamodb
from pytest_lazyfixture import lazy_fixture

from .model_fixtures import CustomFieldsPynamodbModel, CustomFieldsSqliteModel


@pytest.fixture()
def sqlite_adapter_test_table():
    yield CustomFieldsSqliteModel


@pytest.fixture()
def pynamodb_adapter_test_table():
    yield CustomFieldsPynamodbModel


@pytest.mark.parametrize(
    'custom_fields_test_table',
    [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))],
)
@mock_dynamodb
def test_timestamp_serialization(custom_fields_test_table):
    if custom_fields_test_table.exists():
        custom_fields_test_table.delete_table()
    custom_fields_test_table.create_table()

    created = datetime(2020, 1, 1, 11, tzinfo=timezone.utc)
    m = custom_fields_test_table(
        hash_key="0A", range_key="XX", custom_list_field=[dict(fldA="ABC", fldB=[0, 2, 3])], created=created
    )

    print(custom_fields_test_table.created.serialize(created))
    print(m.to_simple_dict(force=True))
    print(m.to_dynamodb_dict())

    attr = custom_fields_test_table.created
    assert attr.deserialize(attr.get_value({'N': '1577876400'})) == created


@pytest.mark.parametrize(
    'custom_fields_test_table',
    [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))],
)
@mock_dynamodb
def test_filter_condition_on_numeric_attribute(custom_fields_test_table):

    # because Numeric Fields are in QUERY_ATTRIBUTES
    if custom_fields_test_table.exists():
        custom_fields_test_table.delete_table()
    custom_fields_test_table.create_table()

    created = datetime(2020, 1, 1, 11, tzinfo=timezone.utc)
    m = custom_fields_test_table(
        hash_key="0B", range_key="XX", custom_list_field=[dict(fldA="ABC", fldB=[0, 2, 3])], created=created, number=42
    )

    m.save()

    res = custom_fields_test_table.query(
        hash_key="0B",
        range_key_condition=custom_fields_test_table.range_key == "XX",
        filter_condition=custom_fields_test_table.number == 42,
    )

    result = list(res)
    assert len(result) == 1
    assert result[0].number == 42


@pytest.mark.parametrize(
    'custom_fields_test_table',
    [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))],
)
@mock_dynamodb
def test_filter_condition_on_custom_str_enum(custom_fields_test_table):

    if custom_fields_test_table.exists():
        custom_fields_test_table.delete_table()
    custom_fields_test_table.create_table()

    created = datetime(2020, 1, 1, 11, tzinfo=timezone.utc)
    m = custom_fields_test_table(
        hash_key="0B", range_key="XX", custom_list_field=[dict(fldA="ABC", fldB=[0, 2, 3])], created=created, enum='PGA'
    )

    m.save()

    res = custom_fields_test_table.query(
        hash_key="0B",
        range_key_condition=custom_fields_test_table.range_key == "XX",
        filter_condition=custom_fields_test_table.enum == "PGA",
    )

    result = list(res)
    assert len(result) == 1

    print(result[0])
    assert result[0].enum == "PGA"


@pytest.mark.parametrize(
    'payload, expected',
    [
        (150, 150),
        (0, 0),
    ],
)
@pytest.mark.parametrize(
    'custom_fields_test_table',
    [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))],
)
@mock_dynamodb
def test_filter_condition_on_custom_numeric_enum(payload, expected, custom_fields_test_table):

    if custom_fields_test_table.exists():
        custom_fields_test_table.delete_table()
    custom_fields_test_table.create_table()

    # created = datetime(2020, 1, 1, 11, tzinfo=timezone.utc)
    m = custom_fields_test_table(
        hash_key="0B",
        range_key="XX",
        custom_list_field=[dict(fldA="ABC", fldB=[0, 2, 3])],
        # created=created,
        enum_numeric=payload,
    )

    m.save()

    res = custom_fields_test_table.query(
        hash_key="0B",
        range_key_condition=custom_fields_test_table.range_key == "XX",
        filter_condition=custom_fields_test_table.enum_numeric == payload,
    )

    result = list(res)
    assert len(result) == 1

    print(result[0])
    assert result[0].enum_numeric == expected


# @pytest.mark.skip("wack")
@pytest.mark.parametrize(
    'custom_fields_test_table',
    [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))],
)
@mock_dynamodb
def test_roundtrip_custom_list_of_map(custom_fields_test_table):
    if custom_fields_test_table.exists():
        custom_fields_test_table.delete_table()
    custom_fields_test_table.create_table()

    created = datetime(2020, 1, 1, 11, tzinfo=timezone.utc)
    m = custom_fields_test_table(
        hash_key="0A",
        range_key="XX",
        my_fk=('A', 'A'),
        custom_list_field=[dict(fldA="ABC", fldB=[0, 2, 3])],
        created=created,
    )

    # print("TO:", m.to_dynamodb_dict())
    m.save()

    res = custom_fields_test_table.query(hash_key="0A", range_key_condition=custom_fields_test_table.range_key == "XX")

    result = list(res)
    assert len(result) == 1
    assert type(result[0]) == custom_fields_test_table
    assert result[0].hash_key == "0A"
    assert result[0].range_key == "XX"

    # assert result[0].custom_list_field[0].__class__ == CustomMapAttribute
    assert result[0].custom_list_field[0]['fldA'] == "ABC"
    assert result[0].custom_list_field[0]['fldB'] == [0, 2, 3]
    assert result[0].created == created
    # assert 0


@pytest.mark.parametrize(
    'custom_fields_test_table',
    [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))],
)
@mock_dynamodb
def test_roundtrip_twice_fk(custom_fields_test_table):
    if custom_fields_test_table.exists():
        custom_fields_test_table.delete_table()
    custom_fields_test_table.create_table()

    created = datetime(2020, 1, 1, 11, tzinfo=timezone.utc)
    m = custom_fields_test_table(
        hash_key="0A",
        range_key="XX",
        my_fk=('A', 'A'),
        custom_list_field=[dict(fldA="ABC", fldB=[0, 2, 3])],
        created=created,
    )
    m.save()
    res = custom_fields_test_table.query(hash_key="0A", range_key_condition=custom_fields_test_table.range_key == "XX")
    m1 = next(res)
    m1.custom_list_field = [dict(fldA="XYZ", fldB=[0, 2, 3])]
    # m1.my_fk = ('B', 'M')
    m1.save()
    assert m1.my_fk == ('A', 'A')
    # assert 0
