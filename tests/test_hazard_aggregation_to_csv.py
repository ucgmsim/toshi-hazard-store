"""test csv export from HazardAggregation."""

import csv
import io
import unittest

from moto import mock_dynamodb

from toshi_hazard_store import model, query_v3

from .test_query_hazard_agg_v3 import HAZARD_MODEL_ID, build_hazard_aggregation_models, imts, locs, vs30s


@mock_dynamodb
class QueryHazardAggregationV3Csv(unittest.TestCase):
    def setUp(self):
        model.migrate()
        with model.HazardAggregation.batch_write() as batch:
            for item in build_hazard_aggregation_models():
                batch.save(item)
        super(QueryHazardAggregationV3Csv, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(QueryHazardAggregationV3Csv, self).tearDown()

    def test_query_and_serialise_csv(self):
        qlocs = [loc.downsample(0.001).code for loc in locs[:2]]
        res = list(query_v3.get_hazard_curves(qlocs, vs30s, [HAZARD_MODEL_ID], imts))
        csv_file = io.StringIO()

        writer = csv.writer(csv_file)
        writer.writerows(model.HazardAggregation.to_csv(res))

        csv_file.seek(0)
        header = next(csv_file)
        rows = list(itm for itm in csv_file)
        self.assertTrue(header.startswith('agg,imt,lat,lon,vs30,poe-'))
        self.assertEqual(len(res), len(rows))
        self.assertTrue(
            [rv.val for rv in res[-1].values[-10:]], rows[-1].split(',')[-10:]
        )  # last 10 vals in the last row
