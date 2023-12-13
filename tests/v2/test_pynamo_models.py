import json
import unittest

import pynamodb.exceptions
from moto import mock_dynamodb
# from nzshm_common.location.code_location import CodedLocation

from toshi_hazard_store.v2 import model

def get_one_meta():
    return model.ToshiV2DemoTable(
        hash_key="ToshiOpenquakeMeta",
        hazard_solution_id="AMCDEF",
        general_task_id="GBBSGG",
        range_key="AMCDEF:350",
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

    def test_table_exists(self):
        self.assertEqual(model.ToshiV2DemoTable.exists(), True)

    def test_save_one_meta_object(self):
        obj = get_one_meta()
        obj.save()
        self.assertEqual(obj.vs30, 350)
