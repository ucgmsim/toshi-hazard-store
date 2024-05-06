import random

import pytest
from nzshm_common.location.coded_location import CodedLocation

from toshi_hazard_store import model


@pytest.fixture
def get_one_hazard_aggregate_with_Site_specific_vs30(adapted_hazagg_model):
    lvps = list(map(lambda x: adapted_hazagg_model.LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, 51)))
    location = CodedLocation(lat=-41.3, lon=174.78, resolution=0.001)
    yield lambda: adapted_hazagg_model.HazardAggregation(
        values=lvps,
        agg=model.AggregationEnum.MEAN.value,
        imt="PGA",
        vs30=0,
        site_vs30=random.randint(200, 1000),
        hazard_model_id="HAZ_MODEL_ONE",
    ).set_location(location)


class TestHazardAggregationQuery:
    def test_model_query_equal_condition(self, get_one_hazard_aggregate_with_Site_specific_vs30, adapted_hazagg_model):

        hag = get_one_hazard_aggregate_with_Site_specific_vs30()
        hag.save()

        mHAG = adapted_hazagg_model.HazardAggregation
        range_condition = mHAG.sort_key == '-41.300~174.780:000:PGA:mean:HAZ_MODEL_ONE'
        filter_condition = mHAG.vs30.is_in(0) & mHAG.imt.is_in('PGA') & mHAG.hazard_model_id.is_in('HAZ_MODEL_ONE')

        # query on model
        res = list(
            adapted_hazagg_model.HazardAggregation.query(
                hag.partition_key,
                range_condition,
                filter_condition,
                # model.HazardAggregation.sort_key == '-41.300~174.780:450:PGA:mean:HAZ_MODEL_ONE'
            )
        )[0]
        assert res.partition_key == hag.partition_key
        assert res.sort_key == hag.sort_key
        assert 200 <= res.site_vs30 <= 1000
