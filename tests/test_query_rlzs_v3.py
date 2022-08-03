import itertools
import unittest

from moto import mock_dynamodb
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS_BY_ID

from toshi_hazard_store import model, query_v3

TOSHI_ID = 'FAk3T0sHi1D=='
vs30s = [250, 350, 450]
imts = ['PGA', 'SA(0.5)']
locs = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in LOCATIONS_BY_ID.values()]
rlzs = [x for x in range(5)]
# lvps = list(map(lambda x: model.LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, 51)))

lat = -41.3
lon = 174.78


def build_rlzs_v3_models():
    """New realization handles all the IMT levels."""
    # imtvs = []
    # for t in ['PGA', 'SA(0.5)', 'SA(1.0)']:
    #     levels = range(1, 51)
    #     values = range(101, 151)
    #     imtvs.append(model.IMTValuesAttribute(imt="PGA", lvls=levels, vals=values))

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
class QueryRlzsV3Test(unittest.TestCase):
    def setUp(self):
        model.migrate()
        with model.OpenquakeRealization.batch_write() as batch:
            for item in build_rlzs_v3_models():
                batch.save(item)
        super(QueryRlzsV3Test, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(QueryRlzsV3Test, self).tearDown()

    def test_query_rlzs_objects(self):
        qlocs = [loc.downsample(0.001).code for loc in locs[:1]]
        print(f'qlocs {qlocs}')
        res = list(query_v3.get_rlz_curves_v3(qlocs, vs30s, rlzs, [TOSHI_ID], imts))
        print(res)
        self.assertEqual(len(res), len(rlzs) * len(vs30s) * len(locs[:1]))
        self.assertEqual(res[0].nloc_001, qlocs[0])

    # def test_query_rlzs_objects_2(self):

    #     res = list(query.get_hazard_rlz_curves_v3(TOSHI_ID, ['PGA'], ['WLG', 'QZN'], None))
    #     print(res)
    #     self.assertEqual(len(res), len(rlzs) * 2)
    #     self.assertEqual(res[0].loc, 'QZN')
    #     self.assertEqual(res[len(rlzs)].loc, 'WLG')

    # def test_query_rlzs_objects_3(self):

    #     res = list(query.get_hazard_rlz_curves_v3(TOSHI_ID, ['PGA'], None, None))
    #     print(res)
    #     self.assertEqual(len(res), len(rlzs) * len(locs))

    # def test_query_rlzs_objects_4(self):

    #     res = list(query.get_hazard_rlz_curves_v3(TOSHI_ID, ['PGA'], ['WLG', 'QZN'], ['001']))
    #     print(res)
    #     self.assertEqual(len(res), 2)
    #     self.assertEqual(res[0].loc, 'QZN')
    #     self.assertEqual(res[1].loc, 'WLG')
    #     self.assertEqual(res[0].rlz, '001')

    # def test_query_rlzs_objects_all(self):

    #     res = list(query.get_hazard_rlz_curves_v3(TOSHI_ID))
    #     print(res)
    #     self.assertEqual(len(res), len(list(build_rlzs_v3_models())))
    #     self.assertEqual(res[0].loc, 'QZN')
