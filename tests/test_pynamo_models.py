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
        models.ToshiHazardCurveObject.create_table(wait=True)
        super(PynamoTest, self).setUp()

    def test_table_exists(self):
        self.assertEqual(models.ToshiHazardCurveObject.exists(), True)

    # def test_get_rupture_ids(self):

    #     #with self.app.app_context():
    #     dataframe = model.SolutionLocationRadiusRuptureSet(
    #         solution_id = 'test_solution_id',
    #         location_radius = 'WLG:10000',
    #         radius =  10000,
    #         location = 'WLG',
    #         ruptures = [1,2,3],
    #         rupture_count = 3
    #         )
    #     dataframe.save()

    #     ids = get_rupture_ids(solution_id='test_solution_id', locations=['WLG'], radius=10000)

    #     self.assertEqual(len(ids), 3)
    #     self.assertEqual(ids, set([1,2,3]))

    def tearDown(self):
        models.ToshiHazardCurveObject.delete_table()
        return super(PynamoTest, self).tearDown()


if __name__ == '__main__':
    unittest.main()
