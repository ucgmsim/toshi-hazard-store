import itertools
import unittest

from moto import mock_dynamodb
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID

from toshi_hazard_store import model, query_v3

TOSHI_ID = 'FAk3T0sHi1D=='
vs30s = [250, 500, 1000, 1500]
imts = ['PGA']
locs = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in list(LOCATIONS_BY_ID.values())[:2]]
rlzs = [x for x in range(5)]

lat = -41.3
lon = 174.78


def build_rlzs_v3_models():
    """New realization handles all the IMT levels."""

    n_lvls = 29
    for rlz in rlzs:
        values = []
        for imt, val in enumerate(imts):
            values.append(
                model.IMTValuesAttribute(
                    imt=val,
                    lvls=[x / 1e3 for x in range(1, n_lvls)],
                    vals=[x / 1e6 for x in range(1, n_lvls)],
                )
            )
        for (loc, rlz, vs30) in itertools.product(locs[:5], rlzs, vs30s):
            # yield model.OpenquakeRealization(loc=loc, rlz=rlz, values=imtvs, lat=lat, lon=lon)
            rlz = model.OpenquakeRealization(
                values=values,
                rlz=rlz,
                vs30=vs30,
                hazard_solution_id=TOSHI_ID,
                source_tags=['TagOne'],
                source_ids=['Z', 'XX'],
            )
            rlz.set_location(loc)
            yield rlz


@mock_dynamodb
class QueryRlzsVs30Test(unittest.TestCase):
    def setUp(self):
        model.migrate()
        with model.OpenquakeRealization.batch_write() as batch:
            for item in build_rlzs_v3_models():
                batch.save(item)
        super(QueryRlzsVs30Test, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(QueryRlzsVs30Test, self).tearDown()

    def test_query_rlzs_objects(self):
        qlocs = [loc.downsample(0.001).code for loc in locs]
        print(f'qlocs {qlocs}')
        res = list(query_v3.get_rlz_curves_v3(qlocs, vs30s, rlzs, [TOSHI_ID], imts))
        print(res)
        self.assertEqual(len(res), len(rlzs) * len(vs30s) * len(locs))
        self.assertEqual(res[0].nloc_001, qlocs[0])

    def test_query_hazard_aggr_with_vs30_mixed_B(self):
        vs30s = [500, 1000]
        qlocs = [loc.downsample(0.001).code for loc in locs]
        res = list(query_v3.get_rlz_curves_v3(qlocs, vs30s, rlzs, [TOSHI_ID], imts))
        self.assertEqual(len(res), len(rlzs) * len(vs30s) * len(locs))

    def test_query_hazard_aggr_with_vs30_one_long(self):
        vs30s = [1500]
        qlocs = [loc.downsample(0.001).code for loc in locs]
        res = list(query_v3.get_rlz_curves_v3(qlocs, vs30s, rlzs, [TOSHI_ID], imts))
        self.assertEqual(len(res), len(rlzs) * len(vs30s) * len(locs))

    def test_query_hazard_aggr_with_vs30_two_long(self):
        vs30s = [1000, 1500]
        qlocs = [loc.downsample(0.001).code for loc in locs]
        res = list(query_v3.get_rlz_curves_v3(qlocs, vs30s, rlzs, [TOSHI_ID], imts))
        self.assertEqual(len(res), len(rlzs) * len(vs30s) * len(locs))

    def test_query_hazard_aggr_with_vs30_one_short(self):
        vs30s = [500]
        qlocs = [loc.downsample(0.001).code for loc in locs]
        res = list(query_v3.get_rlz_curves_v3(qlocs, vs30s, rlzs, [TOSHI_ID], imts))
        self.assertEqual(len(res), len(rlzs) * len(vs30s) * len(locs))

    def test_query_hazard_aggr_with_vs30_two_short(self):
        vs30s = [250, 500]
        qlocs = [loc.downsample(0.001).code for loc in locs]
        res = list(query_v3.get_rlz_curves_v3(qlocs, vs30s, rlzs, [TOSHI_ID], imts))
        self.assertEqual(len(res), len(rlzs) * len(vs30s) * len(locs))
