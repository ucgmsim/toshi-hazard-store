### flake8: noqa
import unittest

from moto import mock_dynamodb
from nzshm_oq_export import models

# from api.datastore.datastore import get_datastore
# from api.datastore.solvis_db_query import get_rupture_ids
# #
# from api.datastore.solvis_db import get_location_radius_rupture_models
# from api.tests.test_api_location_list import TestResources


@mock_dynamodb
class PynamoTest(unittest.TestCase):
    def setUp(self):

        # models.set_local_mode()
        models.ToshiHazardCurveRlzsObject.create_table(wait=True)
        super(PynamoTest, self).setUp()

    def test_table_exists(self):
        self.assertEqual(models.ToshiHazardCurveRlzsObject.exists(), True)

    def test_save_one_object(self):

        lvps = list(map(lambda x: models.LevelValuePairAttribute(level=x / 1e3, value=(x / 1e6)), range(1, 51)))
        print(lvps)

        obj = models.ToshiHazardCurveRlzsObject(
            hazard_solution_id="ABCDE", loc_imt_rk="WLG:PGA", location_code="WLG", imt_code="PGA", lvl_val_pairs=lvps
        )

        print(f'obj: {obj} {obj.version}')
        obj.save()
        print(f'obj: {obj} {obj.version}')
        print(dir(obj))

        self.assertEqual(obj.lvl_val_pairs[0].level, 0.001)
        self.assertEqual(obj.lvl_val_pairs[0].value, 0.000001)
        self.assertEqual(obj.lvl_val_pairs[9].level, 0.01)
        self.assertEqual(obj.lvl_val_pairs[9].value, 0.00001)

    def tearDown(self):
        models.ToshiHazardCurveRlzsObject.delete_table()
        return super(PynamoTest, self).tearDown()
