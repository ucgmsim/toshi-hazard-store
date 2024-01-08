# from moto import mock_dynamodb
# from nzshm_common.location.code_location import CodedLocation
import os

import pytest
from moto import mock_dynamodb
from pytest_lazyfixture import lazy_fixture


def test_env(tmp_path):
    assert os.environ["THS_SQLITE_FOLDER"] == str(tmp_path)


@pytest.mark.parametrize(
    'adapter_test_table', [(lazy_fixture('sqlite_adapter_test_table')), (lazy_fixture('pynamodb_adapter_test_table'))]
)
@mock_dynamodb
def test_table_batch_save(adapter_test_table):
    if adapter_test_table.exists():
        adapter_test_table.delete_table()
    adapter_test_table.create_table()

    with adapter_test_table.batch_write() as batch:
        for rk in range(26):
            itm = adapter_test_table(my_hash_key="ABD123", my_range_key=f"qwerty123-{rk:{0}3}")
            batch.save(itm)

    res = adapter_test_table.query(
        hash_key="ABD123",
    )
    result = list(res)
    assert len(result) == 26
    assert result[25].my_range_key == "qwerty123-025"
