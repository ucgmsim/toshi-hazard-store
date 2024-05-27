import os
import unittest

# import pytest
from pathlib import Path

import numpy as np
from moto import mock_dynamodb
from nzshm_common.location.coded_location import CodedLocation

from toshi_hazard_store import model

folder = Path(Path(os.path.realpath(__file__)).parent, 'fixtures', 'disaggregation')
disaggs = np.load(Path(folder, 'deagg_SLT_v8_gmm_v2_FINAL_-39.000~175.930_750_SA(0.5)_86_eps-dist-mag-trt.npy'))
bins = np.load(
    Path(folder, 'bins_SLT_v8_gmm_v2_FINAL_-39.000~175.930_750_SA(0.5)_86_eps-dist-mag-trt.npy'), allow_pickle=True
)
shaking_level = 0.1


def get_one_disagg_aggregate():
    location = CodedLocation(lat=-41.3, lon=174.78, resolution=0.01)
    return model.DisaggAggregationExceedance.new_model(
        location=location,
        disaggs=disaggs,
        bins=bins,
        hazard_agg=model.AggregationEnum._90.value,  # 90th percentile hazard
        disagg_agg=model.AggregationEnum.MEAN.value,  # mean dissagg
        probability=model.ProbabilityEnum._10_PCT_IN_50YRS,
        shaking_level=shaking_level,
        imt=model.IntensityMeasureTypeEnum.PGA.value,
        vs30=model.VS30Enum._450.value,
        hazard_model_id="HAZ_MODEL_ONE",
    )


@mock_dynamodb
class PynamoTestDisaggAggregationQuery(unittest.TestCase):
    def setUp(self):

        model.migrate_disagg()
        super(PynamoTestDisaggAggregationQuery, self).setUp()

    def tearDown(self):
        model.drop_disagg()
        return super(PynamoTestDisaggAggregationQuery, self).tearDown()

    def test_model_query_no_condition(self):
        """fetch the single object from tbale and check it's structure OK."""

        dag = get_one_disagg_aggregate()
        dag.save()

        # query on model
        res = list(model.DisaggAggregationExceedance.query(dag.partition_key))[0]

        self.assertEqual(res.partition_key, dag.partition_key)
        self.assertEqual(res.sort_key, dag.sort_key)

        self.assertEqual(res.shaking_level, shaking_level)
        self.assertEqual(res.disaggs.all(), disaggs.all())

        print(res)
        print(res.probability)

        assert res.probability == model.ProbabilityEnum._10_PCT_IN_50YRS
        assert res.imt == model.IntensityMeasureTypeEnum.PGA.value

        for idx in range(len(bins)):
            print(idx, type(bins[idx]))
            if type(bins[idx]) == list:
                assert res.bins[idx] == bins[idx]
            elif type(bins[idx]) == np.ndarray:
                assert res.bins[idx].all() == bins[idx].all()
            else:
                assert res.bins[idx] == bins[idx]

    def test_model_query_equal_condition(self):

        dag = get_one_disagg_aggregate()
        print(dag)

        dag.save()  # FAIL assert self.enum_type[value] # CBC MARKS

        # query on model
        res = list(
            model.DisaggAggregationExceedance.query(
                dag.partition_key,
                range_key_condition=(
                    model.DisaggAggregationExceedance.sort_key
                    == 'HAZ_MODEL_ONE:0.9:mean:-41.300~174.780:450:PGA:_10_PCT_IN_50YRS'
                ),
            )
        )[0]
        self.assertEqual(res.partition_key, dag.partition_key)
        self.assertEqual(res.sort_key, dag.sort_key)
