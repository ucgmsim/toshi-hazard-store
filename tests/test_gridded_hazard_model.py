# import datetime as dt
# import json
import unittest

# from dateutil.tz import tzutc
from moto import mock_dynamodb

from toshi_hazard_store import model

# from nzshm_common.grids import load_grid, RegionGrid


@mock_dynamodb
class PynamoTest(unittest.TestCase):
    def setUp(self):

        model.migrate()
        super(PynamoTest, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(PynamoTest, self).tearDown()

    def test_table_exists(self):
        self.assertEqual(model.GriddedHazard.exists(), True)

    def test_save_one_gridded_hazard_object(self):

        obj = model.GriddedHazard.new_model(
            hazard_model_id="SOMESUCH",
            location_grid_id="NZGRID",
            vs30=400,
            imt='PGA',
            agg='0.995',
            poe=0.02,
            grid_poes=[1.0, 2.0, 3.0],
        )

        print(f'obj: {obj} {obj.version}')
        obj.save()
        print(f'obj: {obj} {obj.version}')
        print(dir(obj))

        self.assertEqual(obj.grid_poes[0], 1.0)
