import pytest
from moto import mock_dynamodb
import json, base64
import pickle
from pytest_lazyfixture import lazy_fixture
from datetime import datetime, timezone
from pynamodb.models import Model
from enum import Enum

from pynamodb_attributes import IntegerAttribute, TimestampAttribute

from pynamodb.attributes import UnicodeAttribute, ListAttribute, MapAttribute, NumberAttribute

from toshi_hazard_store.model.attributes import EnumConstrainedUnicodeAttribute, EnumConstrainedIntegerAttribute

from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter


class CustomMapAttribute(MapAttribute):
    fldA = UnicodeAttribute()
    fldB = ListAttribute(of=NumberAttribute)


class SomeEnum(Enum):
    PGA = 'PGA'
    SA_0_1 = 'SA(0.1)'


class NumericEnum(Enum):
    _0 = 0  # indicates that this value is not used
    _150 = 150
    _175 = 175


class FieldsMixin:
    hash_key = UnicodeAttribute(hash_key=True)
    range_key = UnicodeAttribute(range_key=True)
    # custom_field = CustomMapAttribute()
    custom_list_field = ListAttribute(of=CustomMapAttribute)
    created = TimestampAttribute(default=datetime.now(tz=timezone.utc))
    number = NumberAttribute(null=True)

    enum = EnumConstrainedUnicodeAttribute(SomeEnum, null=True)
    enum_numeric = EnumConstrainedIntegerAttribute(NumericEnum, null=True)


class CustomFieldsSqliteModel(FieldsMixin, SqliteAdapter, Model):
    class Meta:
        table_name = "MySQLITEModel"


class CustomFieldsPynamodbModel(FieldsMixin, Model):
    class Meta:
        table_name = "MyPynamodbModel"
        region = "us-east-1"


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
        hash_key="0A", range_key="XX", custom_list_field=[dict(fldA="ABC", fldB=[0, 2, 3])], created=created
    )

    # print("TO:", m.to_dynamodb_dict())
    m.save()

    res = custom_fields_test_table.query(hash_key="0A", range_key_condition=custom_fields_test_table.range_key == "XX")

    result = list(res)
    assert len(result) == 1
    assert type(result[0]) == custom_fields_test_table
    assert result[0].hash_key == "0A"
    assert result[0].range_key == "XX"

    assert result[0].custom_list_field[0].__class__ == CustomMapAttribute
    assert result[0].custom_list_field[0].fldA == "ABC"
    assert result[0].custom_list_field[0].fldB == [0, 2, 3]
    assert result[0].created == created
