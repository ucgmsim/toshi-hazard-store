from uuid import uuid4

import pytest
from moto import mock_dynamodb
from pynamodb.attributes import (  # NumberAttribute,; UnicodeSetAttribute,
    ListAttribute,
    MapAttribute,
    UnicodeAttribute,
    VersionAttribute,
)
from pynamodb.models import Model
from pytest_lazyfixture import lazy_fixture


# These tests are from https://pynamodb.readthedocs.io/en/stable/optimistic_locking.html#version-attribute
class OfficeEmployeeMap(MapAttribute):
    office_employee_id = UnicodeAttribute()
    person = UnicodeAttribute()

    def __eq__(self, other):
        return isinstance(other, OfficeEmployeeMap) and self.person == other.person


class Office(Model):
    class Meta:
        table_name = 'Office'
        region = "us-east-1"

    office_id = UnicodeAttribute(hash_key=True)
    employees = ListAttribute(of=OfficeEmployeeMap)
    name = UnicodeAttribute()
    version = VersionAttribute()


def test_as_writ():
    with mock_dynamodb():
        Office.create_table()
        justin = OfficeEmployeeMap(office_employee_id=str(uuid4()), person='justin')
        garrett = OfficeEmployeeMap(office_employee_id=str(uuid4()), person='garrett')
        office = Office(office_id=str(uuid4()), name="office", employees=[justin, garrett])
        office.save()
        assert office.version == 1

        # Get a second local copy of Office
        office_out_of_date = Office.get(office.office_id)

        # Add another employee and persist the change.
        office.employees.append(OfficeEmployeeMap(office_employee_id=str(uuid4()), person='lita'))
        office.save()

        # On subsequent save or update operations the version is also incremented locally to match
        # the persisted value so there's no need to refresh between operations when reusing the local copy.
        assert office.version == 2
        assert office_out_of_date.version == 1


@pytest.mark.parametrize(
    'adapter_test_table',
    [(lazy_fixture('sqlite_adapter_test_table_versioned')), (lazy_fixture('pynamodb_adapter_test_table_versioned'))],
)
@mock_dynamodb
def test_versioned_my_as_writ(adapter_test_table):

    if adapter_test_table.exists():
        adapter_test_table.delete_table()
    adapter_test_table.create_table()

    itm0 = adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty", my_payload="X")
    itm0.save()
    assert itm0.version == 1

    itm0.my_payload = "XXX"
    itm0.save()
    assert itm0.version == 2
    assert itm0.my_payload == "XXX"
    # imt1 = adapter_test_table(my_hash_key="ABD123", my_range_key="123", my_payload="X")
    # imt1 =
    # itm0.save()


@pytest.mark.parametrize(
    'adapter_test_table',
    [(lazy_fixture('sqlite_adapter_test_table_versioned')), (lazy_fixture('pynamodb_adapter_test_table_versioned'))],
)
@mock_dynamodb
def test_versioned_my_as_writ_query(adapter_test_table):

    if adapter_test_table.exists():
        adapter_test_table.delete_table()
    adapter_test_table.create_table()

    itm0 = adapter_test_table(my_hash_key="ABD123", my_range_key="qwerty", my_payload="X")
    itm0.save()
    assert itm0.version == 1

    itm0.my_payload = "XXX"
    itm0.save()
    assert itm0.version == 2
    assert itm0.my_payload == "XXX"

    res = adapter_test_table.query(hash_key="ABD123", range_key_condition=adapter_test_table.my_range_key == "qwerty")

    itm1 = next(res)
    assert itm1.version == 2
    assert itm1.my_payload == "XXX"

    itm1.my_payload == "QQQ"
    itm1.save()
    assert itm1.version == 3
