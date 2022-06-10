import datetime as dt
import itertools
import json
import unittest

from dateutil.tz import tzutc
from moto import mock_dynamodb

from toshi_hazard_store import model, query

TOSHI_IDS = ['FAk3T0sHi1D==', 'VKK001==']
vs30s = [250, 350, 450]
imts = ['PGA', 'SA(0.5)']
locs = ['WLG', 'QZN']


def build_meta_object(toshi_id, vs30):
    obj = model.ToshiOpenquakeHazardMeta(
        partition_key="ToshiOpenquakeHazardMeta",
        updated=dt.datetime.now(tzutc()),
        # known at configuration
        vs30=vs30,  # vs30 value
        haz_sol_id=toshi_id,
        imts=['PGA', 'SA(0.5)'],  # list of IMTs
        locs=['WLG', 'AKL'],  # list of Location codes
        srcs=['A', 'B'],  # list of source model ids
        aggs=['0.1', '0.5', '0.9', 'mean'],
        inv_time=1.0,
        # extracted from the OQ HDF5
        src_lt=json.dumps(dict(sources=[1, 2])),  # sources meta as DataFrame JSON
        gsim_lt=json.dumps(dict(gsims=[1, 2])),  # gmpe meta as DataFrame JSON
        rlz_lt=json.dumps(dict(rlzs=[1, 2])),  # realization meta as DataFrame JSON
    )
    obj.hazsol_vs30_rk = f"{obj.haz_sol_id}:{obj.vs30}"
    return obj


def build_test_metas():
    for (toshi_id, vs30) in itertools.product(TOSHI_IDS, vs30s):
        m = build_meta_object(toshi_id, vs30)
        m.save()


@mock_dynamodb
class QueryModuleTest(unittest.TestCase):
    def setUp(self):

        model.migrate()
        super(QueryModuleTest, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(QueryModuleTest, self).tearDown()

    def test_partial_meta(self):
        obj = model.ToshiOpenquakeHazardMeta(
            partition_key="ToshiOpenquakeHazardMeta",
            updated=dt.datetime.now(tzutc()),
            # known at configuration
            vs30=760.0,  # vs30 value
            haz_sol_id="YABBA",
            imts=['PGA', 'SA(0.5)'],  # list of IMTs
            locs=['WLG', 'AKL'],  # list of Location codes
            srcs=['A', 'B'],  # list of source model ids
            aggs=['0.1', '0.5', '0.9', 'mean'],
            inv_time=1.0,
            # extracted from the OQ HDF5
            src_lt="{}",  # json.dumps(dict()),  # sources meta as DataFrame JSON
            gsim_lt="{}",  # json.dumps(dict()),  # gmpe meta as DataFrame JSON
            rlz_lt="{}",  # json.dumps(dict()),  # realization meta as DataFrame JSON
        )
        obj.hazsol_vs30_rk = f"{obj.haz_sol_id}:{obj.vs30}"
        obj.save()

        saved = model.ToshiOpenquakeHazardMeta.scan()
        self.assertEqual(len(list(saved)), 1)

    def test_batch_save_meta_objects(self):
        self.assertEqual(model.ToshiOpenquakeHazardMeta.exists(), True)

        for (toshi_id, vs30) in itertools.product(TOSHI_IDS, vs30s):
            m = build_meta_object(toshi_id, vs30)
            m.save()

        saved = model.ToshiOpenquakeHazardMeta.scan()
        self.assertEqual(len(list(saved)), 6)

        saved = list(model.ToshiOpenquakeHazardMeta.query("ToshiOpenquakeHazardMeta"))
        for obj in saved:
            print(obj)
        self.assertEqual(len(list(saved)), 6)

    def test_query_meta_objects_all_specific(self):
        build_test_metas()
        meta = list(query.get_hazard_metadata(TOSHI_IDS, vs30s))
        for obj in meta:
            print(obj)
        self.assertEqual(len(list(meta)), 6)

    def test_query_meta_objects_one_toshi_id(self):
        build_test_metas()
        meta = list(query.get_hazard_metadata([TOSHI_IDS[0]]))
        self.assertEqual(len(list(meta)), len(vs30s))
        self.assertEqual(meta[0].vs30, vs30s[0])

    def test_query_meta_objects_one_vs30(self):
        build_test_metas()
        meta = list(query.get_hazard_metadata(None, [vs30s[0]]))
        self.assertEqual(len(list(meta)), len(TOSHI_IDS))
        self.assertEqual(meta[0].vs30, vs30s[0])
        self.assertEqual(meta[0].haz_sol_id, TOSHI_IDS[0])

    def test_query_meta_objects_all(self):
        build_test_metas()
        meta = list(query.get_hazard_metadata())
        self.assertEqual(len(list(meta)), len(vs30s) * len(TOSHI_IDS))

    def test_query_meta_aggs_attribute(self):
        build_test_metas()
        meta = list(query.get_hazard_metadata())
        self.assertTrue('0.1' in meta[0].aggs)

    def test_query_meta_inv_time_attribute(self):
        build_test_metas()
        meta = list(query.get_hazard_metadata())
        self.assertTrue(meta[0].inv_time == 1.0)
