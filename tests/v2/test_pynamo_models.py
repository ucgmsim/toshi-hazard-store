import unittest

# from nzshm_common.location.code_location import CodedLocation
import pytest
from moto import mock_dynamodb

from toshi_hazard_store.v2 import model


def get_one_meta():
    return model.ToshiV2DemoTable(
        hash_key="ToshiOpenquakeMeta",
        range_key="AMCDEF:350",
        hazard_solution_id="AMCDEF",
        general_task_id="GBBSGG",
        vs30=350,  # vs30 value
    )


@mock_dynamodb
class PynamoTestMeta(unittest.TestCase):
    def setUp(self):
        model.migrate()
        super(PynamoTestMeta, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(PynamoTestMeta, self).tearDown()

    @pytest.mark.skip('not ready')
    def test_table_exists(self):
        self.assertEqual(model.ToshiV2DemoTable.exists(), True)

    @pytest.mark.skip('not ready')
    def test_save_one_meta_object(self):
        obj = get_one_meta()
        obj.save()
        self.assertEqual(obj.vs30, 350)
