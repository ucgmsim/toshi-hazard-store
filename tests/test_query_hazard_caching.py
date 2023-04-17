import itertools
import pathlib
import random
import tempfile
import unittest
from unittest.mock import patch

from moto import mock_dynamodb
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID

from toshi_hazard_store import model, query
from toshi_hazard_store.model.caching import cache_store

HAZARD_MODEL_ID = 'MODEL_THE_FIRST'
vs30s = [250, 350, 450]
imts = ['PGA', 'SA(0.5)']
aggs = [model.AggregationEnum.MEAN.value, model.AggregationEnum._10.value]
locs = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in LOCATIONS_BY_ID.values()]


# folder = pathlib.PurePath(os.path.realpath(__file__)).parent
folder = tempfile.TemporaryDirectory()


def tearDown():
    folder.cleanup()


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


@mock_dynamodb
class TestGetHazardCurvesCached(unittest.TestCase):
    @patch("toshi_hazard_store.model.openquake_models.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.LOCAL_CACHE_FOLDER", str(folder.name))
    def setUp(self):
        model.migrate()
        assert pathlib.Path(folder.name).exists()
        with model.HazardAggregation.batch_write() as batch:
            for item in build_hazard_aggregation_models():
                batch.save(item)
        super(TestGetHazardCurvesCached, self).setUp()

    @patch("toshi_hazard_store.model.openquake_models.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.LOCAL_CACHE_FOLDER", str(folder.name))
    def tearDown(self):
        model.drop_tables()
        return super(TestGetHazardCurvesCached, self).tearDown()

    @patch("toshi_hazard_store.model.openquake_models.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.LOCAL_CACHE_FOLDER", str(folder.name))
    def test_query_hazard_curves_cache_population(self):
        qlocs = [loc.downsample(0.001).code for loc in locs[:2]]
        print(f'qlocs {qlocs}')

        res0 = list(query.get_hazard_curves(qlocs, vs30s, [HAZARD_MODEL_ID], imts))
        self.assertEqual(len(res0), len(imts) * len(aggs) * len(vs30s) * len(locs[:2]))
        self.assertEqual(res0[0].nloc_001, qlocs[0])

        res1 = list(query.get_hazard_curves(qlocs, vs30s, [HAZARD_MODEL_ID], imts))
        self.assertEqual(len(res1), len(imts) * len(aggs) * len(vs30s) * len(locs[:2]))

        assert res0[0].sort_key == res1[0].sort_key
        assert res0[0].vs30 == res1[0].vs30
        assert res0[0].imt == res1[0].imt
        assert res0[0].nloc_001 == res1[0].nloc_001
        assert res0[0].agg == res1[0].agg
        assert res0[0].site_vs30 == res1[0].site_vs30
        assert res1[0].site_vs30 is None


@mock_dynamodb
class TestCacheStore(unittest.TestCase):
    @patch("toshi_hazard_store.model.openquake_models.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.LOCAL_CACHE_FOLDER", str(folder.name))
    def setUp(self):
        model.migrate()  # we do this so we get a cache table
        n_lvls = 29
        lvps = list(map(lambda x: model.LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, n_lvls)))
        loc = CodedLocation(-43.2, 177.27, 0.001)
        self.m = model.HazardAggregation(
            values=lvps,
            vs30=700,
            agg='mean',
            imt='PGA',
            hazard_model_id="HAZ_MODEL_ONE",
        ).set_location(loc)
        # model.drop_tables()

    # def tearDown(self):
    #     model.drop_tables()
    #     folder.cleanup()
    #     return super(TestCacheStore, self).tearDown()

    @patch("toshi_hazard_store.model.openquake_models.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.LOCAL_CACHE_FOLDER", str(folder.name))
    def test_cache_put(self):
        mHAG = model.HazardAggregation
        mHAG.create_table(wait=True)
        conn = cache_store.get_connection(model_class=mHAG)
        cache_store.put_model(conn, self.m)

        # now query
        range_condition = model.HazardAggregation.sort_key >= '-43.200~177.270:700:PGA'
        filter_condition = mHAG.vs30.is_in(700) & mHAG.imt.is_in('PGA') & mHAG.hazard_model_id.is_in('HAZ_MODEL_ONE')

        m2 = next(
            cache_store.get_model(
                conn, model_class=mHAG, range_key_condition=range_condition, filter_condition=filter_condition
            )
        )

        assert self.m.sort_key == m2.sort_key
        # assert self.m.created == m2.created
        # assert self.m.values == m2.values TODO
        assert self.m.vs30 == m2.vs30
        assert self.m.imt == m2.imt
        assert self.m.nloc_001 == m2.nloc_001
        assert self.m.agg == m2.agg
        assert self.m.site_vs30 == m2.site_vs30  # new optional attribute
        assert self.m.site_vs30 is None


@mock_dynamodb
class TestCacheStoreWithOptionalAttribute(unittest.TestCase):
    @patch("toshi_hazard_store.model.openquake_models.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.LOCAL_CACHE_FOLDER", str(folder.name))
    def setUp(self):
        model.migrate()  # we do this so we get a cache table
        n_lvls = 29
        lvps = list(map(lambda x: model.LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, n_lvls)))
        loc = CodedLocation(-43.2, 177.27, 0.001)
        self.m = model.HazardAggregation(
            values=lvps,
            vs30=0,
            site_vs30=random.randint(200, 300),
            agg='mean',
            imt='PGA',
            hazard_model_id="HAZ_MODEL_ONE",
        ).set_location(loc)
        # model.drop_tables()

    # def tearDown(self):
    #     model.drop_tables()
    #     folder.cleanup()
    #     return super(TestCacheStore, self).tearDown()

    @patch("toshi_hazard_store.model.openquake_models.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.DEPLOYMENT_STAGE", "MOCK")
    @patch("toshi_hazard_store.model.caching.cache_store.LOCAL_CACHE_FOLDER", str(folder.name))
    def test_cache_put(self):
        mHAG = model.HazardAggregation
        mHAG.create_table(wait=True)
        conn = cache_store.get_connection(model_class=mHAG)
        cache_store.put_model(conn, self.m)

        # now query
        range_condition = model.HazardAggregation.sort_key >= '-43.200~177.270:000:PGA'
        filter_condition = mHAG.vs30.is_in(0) & mHAG.imt.is_in('PGA') & mHAG.hazard_model_id.is_in('HAZ_MODEL_ONE')

        m2 = next(
            cache_store.get_model(
                conn, model_class=mHAG, range_key_condition=range_condition, filter_condition=filter_condition
            )
        )

        assert self.m.sort_key == m2.sort_key
        # assert self.m.created == m2.created
        # assert self.m.values == m2.values TODO
        assert self.m.vs30 == m2.vs30
        assert self.m.imt == m2.imt
        assert self.m.nloc_001 == m2.nloc_001
        assert self.m.agg == m2.agg
        assert self.m.site_vs30 == m2.site_vs30
        assert 200 <= m2.site_vs30 < 300

    # @patch("toshi_hazard_store.model.openquake_models.DEPLOYMENT_STAGE", "MOCK")
    # @patch("toshi_hazard_store.model.caching.cache_store.DEPLOYMENT_STAGE", "MOCK")
    # @patch("toshi_hazard_store.model.caching.cache_store.LOCAL_CACHE_FOLDER", str(folder.name))
    # def test_cache_auto_population(self):
    #     # 2nd pass of same query should use the cache

    #     qlocs = [loc.downsample(0.001).code for loc in locs[:2]]
    #     print(f'qlocs {qlocs}')
    #     res = list(query_v3.get_hazard_curves(qlocs, vs30s, [HAZARD_MODEL_ID], imts))

    #     m1 = next(
    #         cache_store.get_model(
    #             conn, model_class=mHAG, range_key_condition=range_condition, filter_condition=filter_condition
    #         )
    #     )

    #     m2 = next(
    #         cache_store.get_model(
    #             conn, model_class=mHAG, range_key_condition=range_condition, filter_condition=filter_condition
    #         )
    #     )

    # assert m1.sort_key == m2.sort_key
    # assert m1.vs30 == m2.vs30
    # assert m1.imt == m2.imt
    # assert m1.nloc_001 == m2.nloc_001
    # assert m1.agg == m2.agg
