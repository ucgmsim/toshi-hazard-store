import unittest
import itertools
import pytest
import logging

from unittest.mock import patch

from moto import mock_dynamodb
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID

from toshi_hazard_store import model, query_v3


log = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)
logging.getLogger('toshi_hazard_store').setLevel(logging.DEBUG)

HAZARD_MODEL_ID = 'MODEL_THE_FIRST'
vs30s = [400]
imts = ['PGA']
motion_comps = [model.ComponentEnum.ROTD50, model.ComponentEnum.LHC]
aggs = [model.AggregationEnum.MEAN.value]
locs = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in list(LOCATIONS_BY_ID.values())[:2]]
lat = -41.3
lon = 174.78
location = CodedLocation(lat=lat, lon=lon, resolution=0.001)


def build_hazard_aggregation_models():

    n_lvls = 29
    lvps = list(map(lambda x: model.LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, n_lvls)))
    for (loc, vs30, agg, motion_comp) in itertools.product(locs[:5], vs30s, aggs, motion_comps):
        for imt, val in enumerate(imts):
            yield model.HazardAggregation(
                values=lvps,
                vs30=vs30,
                agg=agg,
                imt=val,
                hazard_model_id=HAZARD_MODEL_ID,
                motion_comp=motion_comp,
            ).set_location(loc)


def get_one_haz_agg_nocomp():
    lvps = list(map(lambda x: model.LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, 51)))
    return model.HazardAggregation(
        values=lvps, agg=model.AggregationEnum.MEAN.value, imt="PGA", vs30=vs30s[0], hazard_model_id=HAZARD_MODEL_ID
    ).set_location(location)


def get_one_haz_agg_wcomp():
    lvps = list(map(lambda x: model.LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, 51)))
    return model.HazardAggregation(
        values=lvps,
        agg=model.AggregationEnum.MEAN.value,
        imt="PGA",
        vs30=vs30s[0],
        hazard_model_id=HAZARD_MODEL_ID,
        motion_comp=model.ComponentEnum.LHC,
    ).set_location(location)


def get_one_haz_agg_error_comp():
    lvps = list(map(lambda x: model.LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, 51)))
    return model.HazardAggregation(
        values=lvps,
        agg=model.AggregationEnum.MEAN.value,
        imt="PGA",
        vs30=450,
        hazard_model_id="HAZ_MODEL_ONE",
        motion_comp="FOOBAR",
    ).set_location(location)


@mock_dynamodb
class SaveHazardAggregationComponentTest(unittest.TestCase):
    def setUp(self):

        model.migrate()
        super(SaveHazardAggregationComponentTest, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(SaveHazardAggregationComponentTest, self).tearDown()

    # test that you can save a hazard agg w/ a component
    def test_save_with_component(self):
        hag = get_one_haz_agg_wcomp()
        hag.save()

        self.assertEqual(hag.motion_comp, model.ComponentEnum.LHC)

    # test that you can save a hazard agg w/o a component (backwards compatability)
    def test_save_without_component(self):
        hag = get_one_haz_agg_nocomp()
        hag.save()

        self.assertEqual(hag.motion_comp, model.ComponentEnum.ROTD50)

    # test that passing a motion component not in the enum thows an error
    def test_save_component_error(self):
        with pytest.raises(AttributeError):
            hag = get_one_haz_agg_error_comp()
            print(hag)

    # test that sort key is correct
    def test_motion_comp_sort_key(self):
        hag = get_one_haz_agg_nocomp()
        hag.save()

        sort_key = f'{location.code}:{vs30s[0]}:{imts[0]}:{aggs[0]}:{HAZARD_MODEL_ID}'
        self.assertEqual(hag.sort_key, sort_key)


@patch("toshi_hazard_store.model.caching.cache_store.LOCAL_CACHE_FOLDER", None)
@mock_dynamodb
class QueryHazardAggregationComponentTest(unittest.TestCase):
    def setUp(self):
        model.migrate()
        with model.HazardAggregation.batch_write() as batch:
            for item in build_hazard_aggregation_models():
                batch.save(item)
        super(QueryHazardAggregationComponentTest, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(QueryHazardAggregationComponentTest, self).tearDown()

    # test that you can retrieve a hazard agg w/o specifying component and get rotd50
    def test_query_no_component(self):
        qlocs = [loc.downsample(0.001).code for loc in locs]
        print(f'qlocs {qlocs}')
        res = list(query_v3.get_hazard_curves(qlocs, vs30s, [HAZARD_MODEL_ID], imts))
        print(res)
        self.assertEqual(res[0].motion_comp, model.ComponentEnum.ROTD50)

    # test that you can retrieve a hazard agg specifying a compoment
    # and that it matches what was requested (both types of component)
    def test_query_with_component(self):
        qlocs = [loc.downsample(0.001).code for loc in locs]
        print(f'qlocs {qlocs}')

        for comp in motion_comps:
            res = list(query_v3.get_hazard_curves(qlocs, vs30s, [HAZARD_MODEL_ID], imts, [], [comp]))
            print(res)
            self.assertEqual(res[0].motion_comp, comp)

        res = list(query_v3.get_hazard_curves(qlocs, vs30s, [HAZARD_MODEL_ID], imts, [], motion_comps))
        self.assertEqual(len(res), len(qlocs) * len(imts) * len(aggs) * len(vs30s) * len(motion_comps))
