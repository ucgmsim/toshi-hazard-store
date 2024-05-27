""""Define some classes and attributes for testing"""

from datetime import datetime, timezone
from enum import Enum

from pynamodb.attributes import (
    ListAttribute,
    MapAttribute,
    NumberAttribute,
    UnicodeAttribute,
    UnicodeSetAttribute,
    VersionAttribute,
)
from pynamodb.models import Model
from pynamodb_attributes import FloatAttribute, TimestampAttribute  # IntegerAttribute,

from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter
from toshi_hazard_store.model.attributes import (
    EnumConstrainedIntegerAttribute,
    EnumConstrainedUnicodeAttribute,
    ForeignKeyAttribute,
)


class FieldsMixin:
    my_hash_key = UnicodeAttribute(hash_key=True)
    my_range_key = UnicodeAttribute(range_key=True)
    my_unicode_set = UnicodeSetAttribute()
    my_float = FloatAttribute(null=True)
    my_payload = UnicodeAttribute(null=True)
    # my_renamed = UnicodeAttribute(null=True, attr_name="ren_and_stimpy")


class VersionedFieldsMixin(FieldsMixin):
    version = VersionAttribute()


class MySqlModel(FieldsMixin, SqliteAdapter, Model):
    class Meta:
        table_name = "MySQLITEModel"
        # region = "us-east-1"


class MyPynamodbModel(FieldsMixin, Model):
    class Meta:
        table_name = "MyPynamodbModel"
        region = "us-east-1"


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


class CustomFieldsMixin:
    hash_key = UnicodeAttribute(hash_key=True)
    range_key = UnicodeAttribute(range_key=True)
    # custom_field = CustomMapAttribute()
    custom_list_field = ListAttribute(of=CustomMapAttribute, null=True)
    my_fk = ForeignKeyAttribute(null=True)
    created = TimestampAttribute(default=datetime.now(tz=timezone.utc))
    number = NumberAttribute(null=True)

    enum = EnumConstrainedUnicodeAttribute(SomeEnum, null=True)
    enum_numeric = EnumConstrainedIntegerAttribute(NumericEnum, null=True)


# below are the versioned test fixtures
class VersionedSqlModel(VersionedFieldsMixin, SqliteAdapter, Model):
    class Meta:
        table_name = "VersionedSqlModel"


class VersionedPynamodbModel(VersionedFieldsMixin, Model):
    class Meta:
        table_name = "VersionedPynamodbModel"
        region = "us-east-1"


class CustomFieldsSqliteModel(CustomFieldsMixin, SqliteAdapter, Model):
    class Meta:
        table_name = "MySQLITEModel"


class CustomFieldsPynamodbModel(CustomFieldsMixin, Model):
    class Meta:
        table_name = "MyPynamodbModel"
        region = "us-east-1"
