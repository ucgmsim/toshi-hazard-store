import itertools
import unittest

from moto import mock_dynamodb

from toshi_hazard_store import model, query

TOSHI_ID = 'FAk3T0sHi1D=='
# vs30s = [250, 350, 450]
imts = ['PGA', 'SA(0.5)']
locs = ['[-41.3~174.78]', 'QZN']
lat = -41.3
lon = 174.78
aggs = ['0.1', 'mean']
lvps = list(map(lambda x: model.LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, 51)))
lat = -41.3
lon = 174.78


def build_stats_models():
    for (imt, loc, agg) in itertools.product(imts, locs, aggs):
        yield model.ToshiOpenquakeHazardCurveStats(loc=loc, imt=imt, agg=agg, values=lvps)


def build_stats_v2_models():
    """New realization handles all the IMT levels."""
    imtvs = []
    for t in ['PGA', 'SA(0.5)', 'SA(1.0)']:
        levels = range(1, 51)
        values = range(101, 151)
        imtvs.append(model.IMTValuesAttribute(imt="PGA", lvls=levels, vals=values))

    for (loc, agg) in itertools.product(locs, aggs):
        yield model.ToshiOpenquakeHazardCurveStatsV2(loc=loc, agg=agg, values=imtvs, lat=lat, lon=lon)


@mock_dynamodb
class QueryStatsV2Test(unittest.TestCase):
    def setUp(self):

        model.migrate()
        query.batch_save_hcurve_stats_v2(TOSHI_ID, models=build_stats_v2_models())
        super(QueryStatsV2Test, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(QueryStatsV2Test, self).tearDown()

    def test_batch_save_stats_objects(self):
        saved = model.ToshiOpenquakeHazardCurveStatsV2.query(TOSHI_ID)
        self.assertEqual(len(list(saved)), len(list(build_stats_v2_models())))

    def test_query_stats_objects(self):

        res = list(query.get_hazard_stats_curves_v2(TOSHI_ID, ['PGA'], ['[-41.3~174.78]'], None))
        print(res)
        self.assertEqual(len(res), len(aggs))
        self.assertEqual(res[0].loc, '[-41.3~174.78]')
        self.assertEqual(res[0].lon, 174.78)
        self.assertEqual(res[0].lat, -41.3)

        self.assertEqual(res[0].agg, '0.1')
        self.assertEqual(res[1].agg, 'mean')

    def test_query_stats_objects_2(self):
        res = list(query.get_hazard_stats_curves_v2(TOSHI_ID, ['PGA'], ['[-41.3~174.78]', 'QZN'], None))
        print(res)
        self.assertEqual(len(res), len(aggs) * 2)
        self.assertEqual(res[0].loc, 'QZN')
        self.assertEqual(res[2].loc, '[-41.3~174.78]')
        self.assertEqual(res[0].agg, '0.1')
        self.assertEqual(res[1].agg, 'mean')

    def test_query_stats_objects_3(self):
        res = list(query.get_hazard_stats_curves_v2(TOSHI_ID, ['PGA'], None, None))
        print(res)
        self.assertEqual(len(res), len(aggs) * len(locs))

    def test_query_stats_objects_4(self):
        res = list(query.get_hazard_stats_curves_v2(TOSHI_ID, ['PGA'], ['[-41.3~174.78]', 'QZN'], ['mean']))
        print(res)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0].loc, 'QZN')
        self.assertEqual(res[1].loc, '[-41.3~174.78]')
        self.assertEqual(res[0].agg, 'mean')
        self.assertEqual(res[1].agg, 'mean')

    def test_query_stats_objects_all(self):
        res = list(query.get_hazard_stats_curves_v2(TOSHI_ID))
        print(res)
        self.assertEqual(len(res), len(list(build_stats_v2_models())))
        self.assertEqual(res[0].loc, 'QZN')
        self.assertEqual(res[0].agg, '0.1')


@mock_dynamodb
class QueryModuleTest(unittest.TestCase):
    def setUp(self):
        model.migrate()
        query.batch_save_hcurve_stats(TOSHI_ID, models=build_stats_models())
        super(QueryModuleTest, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(QueryModuleTest, self).tearDown()

    def test_batch_save_stats_objects(self):
        saved = model.ToshiOpenquakeHazardCurveStats.query(TOSHI_ID)
        self.assertEqual(len(list(saved)), len(list(build_stats_models())))

    def test_query_stats_objects(self):

        res = list(query.get_hazard_stats_curves(TOSHI_ID, ['PGA'], ['[-41.3~174.78]'], None))
        print(res)
        self.assertEqual(len(res), len(aggs))
        # self.assertEqual(res[0].loc, 'WLG')
        self.assertEqual(res[0].agg, '0.1')
        self.assertEqual(res[1].agg, 'mean')

    def test_query_stats_objects_2(self):
        res = list(query.get_hazard_stats_curves(TOSHI_ID, ['PGA'], ['[-41.3~174.78]', 'QZN'], None))
        print(res)
        self.assertEqual(len(res), len(aggs) * 2)
        self.assertEqual(res[0].loc, 'QZN')
        self.assertEqual(res[2].loc, '[-41.3~174.78]')
        self.assertEqual(res[0].agg, '0.1')
        self.assertEqual(res[1].agg, 'mean')

    def test_query_stats_objects_3(self):
        res = list(query.get_hazard_stats_curves(TOSHI_ID, ['PGA'], None, None))
        print(res)
        self.assertEqual(len(res), len(aggs) * len(locs))

    def test_query_stats_objects_4(self):
        res = list(query.get_hazard_stats_curves(TOSHI_ID, ['PGA'], ['[-41.3~174.78]', 'QZN'], ['mean']))
        print(res)
        self.assertEqual(len(res), 2)
        self.assertEqual(res[0].loc, 'QZN')
        # self.assertEqual(res[1].loc, 'WLG')
        self.assertEqual(res[0].agg, 'mean')
        self.assertEqual(res[1].agg, 'mean')

    def test_query_stats_objects_all(self):
        res = list(query.get_hazard_stats_curves(TOSHI_ID))
        print(res)
        self.assertEqual(len(res), len(list(build_stats_models())))
        self.assertEqual(res[0].loc, 'QZN')
        self.assertEqual(res[0].agg, '0.1')
