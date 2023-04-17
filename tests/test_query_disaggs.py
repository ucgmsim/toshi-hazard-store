import itertools
import unittest

import numpy as np
from moto import mock_dynamodb
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID

from toshi_hazard_store import model, query

HAZARD_MODEL_ID = ' MODEL_THE_FIRST'
vs30s = [250, 350]
imts = [model.IntensityMeasureTypeEnum.PGA.value, model.IntensityMeasureTypeEnum.SA_0_5.value]
hazard_aggs = [model.AggregationEnum.MEAN.value, model.AggregationEnum._10.value]
disagg_aggs = [model.AggregationEnum.MEAN.value]
locs = [CodedLocation(loc['latitude'], loc['longitude'], 0.001) for loc in LOCATIONS_BY_ID.values()]
disaggs = np.ndarray(4, float)  # type: ignore
bins = np.ndarray(4, float)  # type: ignore
shaking_level = 0.1
probability = model.ProbabilityEnum._10_PCT_IN_50YRS


def build_disagg_aggregation_models():
    for (loc, vs30, imt, hazard_agg, disagg_agg) in itertools.product(locs[:5], vs30s, imts, hazard_aggs, disagg_aggs):
        yield model.DisaggAggregationExceedance.new_model(
            hazard_model_id=HAZARD_MODEL_ID,
            location=loc,
            disaggs=disaggs,
            bins=bins,
            vs30=vs30,
            hazard_agg=hazard_agg,
            disagg_agg=disagg_agg,
            probability=probability,
            shaking_level=shaking_level,
            imt=imt,
        )


@mock_dynamodb
class QueryDisaggAggregationsTest(unittest.TestCase):
    def setUp(self):
        model.migrate()
        with model.DisaggAggregationExceedance.batch_write() as batch:
            for item in build_disagg_aggregation_models():
                batch.save(item)
        super(QueryDisaggAggregationsTest, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(QueryDisaggAggregationsTest, self).tearDown()

    def test_query_single_valid_hazard_aggr(self):
        qlocs = [loc.downsample(0.001).code for loc in locs[:1]]

        res = query.get_one_disagg_aggregation(
            HAZARD_MODEL_ID,
            model.AggregationEnum.MEAN,
            model.AggregationEnum.MEAN,
            qlocs[0],
            vs30s[0],
            imts[0],
            model.ProbabilityEnum._10_PCT_IN_50YRS,
        )
        print(res)

        assert res.nloc_001 == qlocs[0]
        assert res.disagg_agg == 'mean'
        assert res.probability == model.ProbabilityEnum._10_PCT_IN_50YRS
        assert res.hazard_model_id == HAZARD_MODEL_ID

    def test_query_single_missing_hazard_aggr(self):
        qlocs = [loc.downsample(0.001).code for loc in locs[:1]]

        res = query.get_one_disagg_aggregation(
            HAZARD_MODEL_ID,
            model.AggregationEnum.MEAN,
            model.AggregationEnum.MEAN,
            qlocs[0],
            vs30s[0],
            imts[0],
            model.ProbabilityEnum._2_PCT_IN_50YRS,
        )
        print(res)
        assert res is None

    def test_query_many_valid_hazard_aggr(self):
        qlocs = [loc.downsample(0.001).code for loc in locs[:2]]

        res = list(
            query.get_disagg_aggregates(
                [HAZARD_MODEL_ID],
                [model.AggregationEnum.MEAN],
                [model.AggregationEnum.MEAN],
                qlocs,
                [vs30s[0]],
                [imts[0]],
                [model.ProbabilityEnum._10_PCT_IN_50YRS],
            )
        )
        assert len(res) == 2
        assert res[0].nloc_001 == qlocs[0]
        assert res[1].nloc_001 == qlocs[1]

    def test_query_many_valid_hazard_aggr_offset_checks_indexing(self):
        qlocs = [loc.downsample(0.001).code for loc in locs[3:5]]

        res = list(
            query.get_disagg_aggregates(
                [HAZARD_MODEL_ID],
                [model.AggregationEnum.MEAN],
                [model.AggregationEnum.MEAN],
                qlocs,
                [vs30s[0]],
                [imts[0]],
                [model.ProbabilityEnum._10_PCT_IN_50YRS],
            )
        )
        assert len(res) == 2
        assert res[0].nloc_001 == qlocs[1]
        assert res[1].nloc_001 == qlocs[0]
