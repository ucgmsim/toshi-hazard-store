import itertools
import unittest
from unittest.mock import patch

from moto import mock_dynamodb
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID

from toshi_hazard_store import model, query_v3

HAZARD_MODEL_ID = 'MODEL_THE_FIRST'
vs30s = [250, 350, 450]
imts = ['PGA', 'SA(0.5)']
aggs = [model.AggregationEnum.MEAN.value, model.AggregationEnum._10.value]
locs = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in LOCATIONS_BY_ID.values()]


def build_hazard_aggregation_models():

    n_lvls = 29
    lvps = list(map(lambda x: model.LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, n_lvls)))
    for (loc, vs30, agg) in itertools.product(locs[:5], vs30s, aggs):
        for imt, val in enumerate(imts):
            yield model.HazardAggregation(
                values=lvps,
                vs30=vs30,
                agg=agg,
                imt=val,
                hazard_model_id=HAZARD_MODEL_ID,
            ).set_location(loc)


@patch("toshi_hazard_store.model.caching.cache_store.LOCAL_CACHE_FOLDER", None)
@mock_dynamodb
class QueryHazardAggregationV3Test(unittest.TestCase):
    def setUp(self):
        model.migrate()
        with model.HazardAggregation.batch_write() as batch:
            for item in build_hazard_aggregation_models():
                batch.save(item)
        super(QueryHazardAggregationV3Test, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(QueryHazardAggregationV3Test, self).tearDown()

    def test_query_hazard_aggr(self):
        qlocs = [loc.downsample(0.001).code for loc in locs[:2]]
        print(f'qlocs {qlocs}')
        res = list(query_v3.get_hazard_curves(qlocs, vs30s, [HAZARD_MODEL_ID], imts))
        print(res)
        self.assertEqual(len(res), len(imts) * len(aggs) * len(vs30s) * len(locs[:2]))
        self.assertEqual(res[0].nloc_001, qlocs[0])

    def test_query_hazard_aggr_2(self):
        qlocs = [loc.downsample(0.001).code for loc in locs[:2]]
        res = list(query_v3.get_hazard_curves(qlocs, vs30s, [HAZARD_MODEL_ID, 'FAKE_ID'], imts))
        print(res)
        self.assertEqual(len(res), len(imts) * len(aggs) * len(vs30s) * len(locs[:2]))
        self.assertEqual(res[0].nloc_001, qlocs[0])

    def test_query_hazard_aggr_single(self):
        qlocs = [loc.downsample(0.001).code for loc in locs[:1]]
        print(f'qlocs {qlocs}')
        res = list(query_v3.get_hazard_curves(qlocs, vs30s[:1], [HAZARD_MODEL_ID], imts[:1], aggs=['mean']))
        print(res)
        self.assertEqual(len(res), 1)
        self.assertEqual(res[0].nloc_001, qlocs[0])
