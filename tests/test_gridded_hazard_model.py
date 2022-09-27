# import datetime as dt
# import json
import logging
import unittest

# from dateutil.tz import tzutc
from moto import mock_dynamodb

from toshi_hazard_store import model, query

log = logging.getLogger()
logging.basicConfig(level=logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('pynamodb').setLevel(logging.DEBUG)
logging.getLogger('toshi_hazard_store').setLevel(logging.DEBUG)

GRID = "NZGRID"
HAZARD_MODELS = "SOMESUCH"


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


@mock_dynamodb
class PynamoTestQuery(unittest.TestCase):
    def setUp(self):
        model.migrate()
        super(PynamoTestQuery, self).setUp()
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

    def tearDown(self):
        model.drop_tables()
        return super(PynamoTestQuery, self).tearDown()

    def test_query_tupled_gridded_hazard_aggr(self):
        res = list(
            query.get_gridded_hazard(
                hazard_model_ids=tuple([HAZARD_MODELS]),
                location_grid_ids=tuple([GRID]),
                vs30s=tuple([400]),
                imts=tuple(['PGA']),
                aggs=tuple(['0.995']),
                poes=tuple([0.02]),
            )
        )

        print(res)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].grid_poes, [1.0, 2.0, 3.0])

    def test_query_one_gridded_hazard_aggr(self):
        res = list(
            query.get_one_gridded_hazard(
                hazard_model_id=HAZARD_MODELS,
                location_grid_id=GRID,
                vs30=400,
                imt='PGA',
                agg='0.995',
                poe=0.02,
            )
        )

        print(res)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].grid_poes, [1.0, 2.0, 3.0])
        self.assertEqual(res[0].agg, '0.995')
