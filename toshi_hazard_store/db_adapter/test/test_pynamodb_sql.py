from datetime import datetime, timezone

import pytest

from toshi_hazard_store.db_adapter.sqlite.pynamodb_sql import SqlWriteAdapter

from .model_fixtures import CustomFieldsSqliteModel


@pytest.mark.parametrize(
    'payload, expected',
    [
        (150, 150),
        (0, 0),
    ],
)
def test_insert_sql(payload, expected):
    created = datetime(2020, 1, 1, 11, tzinfo=timezone.utc)
    m = CustomFieldsSqliteModel(
        hash_key="0B",
        range_key="XX",
        custom_list_field=[dict(fldA="ABC", fldB=[0, 2, 3])],
        created=created,
        enum='PGA',
        enum_numeric=payload,
    )

    wa = SqlWriteAdapter(CustomFieldsSqliteModel)
    statement = wa.insert_statement([m])

    print(statement)
    assert f'"{payload}",' in statement
