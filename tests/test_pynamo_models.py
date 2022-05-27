import unittest
import json

from moto import mock_dynamodb
from toshi_hazard_store import model
import datetime as dt

from dateutil.tz import tzutc


@mock_dynamodb
class PynamoTest(unittest.TestCase):
    def setUp(self):

        # model.set_local_mode()
        # model.ToshiOpenquakeHazardCurveRlzs.create_table(wait=True)
        model.migrate()
        super(PynamoTest, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(PynamoTest, self).tearDown()

    def test_table_exists(self):
        self.assertEqual(model.ToshiOpenquakeHazardCurveRlzs.exists(), True)

    def test_save_one_realization_object(self):

        lvps = list(map(lambda x: model.LevelValuePairAttribute(level=x / 1e3, value=(x / 1e6)), range(1, 51)))
        print(lvps)

        obj = model.ToshiOpenquakeHazardCurveRlzs(
            hazard_solution_id="ABCDE",
            imt_loc_rlz_rk="350:PGA:WLG:rlz-010",
            location_code="WLG",
            rlz_id="rlz-010",
            imt_code="PGA",
            lvl_val_pairs=lvps,
        )

        print(f'obj: {obj} {obj.version}')
        obj.save()
        print(f'obj: {obj} {obj.version}')
        print(dir(obj))

        self.assertEqual(obj.lvl_val_pairs[0].level, 0.001)
        self.assertEqual(obj.lvl_val_pairs[0].value, 0.000001)
        self.assertEqual(obj.lvl_val_pairs[9].level, 0.01)
        self.assertEqual(obj.lvl_val_pairs[9].value, 0.00001)

    def test_save_one_stats_object(self):

        lvps = list(map(lambda x: model.LevelValuePairAttribute(level=x / 1e3, value=(x / 1e6)), range(1, 51)))
        print(lvps)

        obj = model.ToshiOpenquakeHazardCurveStats(
            hazard_solution_id="ABCDE",
            imt_loc_agg_rk="350:SA(0.5):WLG:quantile-0.1",
            location_code="WLG",
            aggregation="quantile-0.1",
            imt_code="SA(0.5)",
            lvl_val_pairs=lvps,
        )

        print(f'obj: {obj} {obj.version}')
        self.assertEqual(obj.version, None)

        obj.save()
        self.assertEqual(obj.version, 1)
        print(dir(obj))

        self.assertEqual(obj.lvl_val_pairs[0].level, 0.001)
        self.assertEqual(obj.lvl_val_pairs[0].value, 0.000001)
        self.assertEqual(obj.lvl_val_pairs[9].level, 0.01)
        self.assertEqual(obj.lvl_val_pairs[9].value, 0.00001)

    def test_save_one_meta_object(self):

        obj = model.ToshiOpenquakeHazardMeta(
            partition_key="ToshiOpenquakeHazardMeta",
            hazard_solution_id="AMCDEF",
            hazsol_vs30_rk="UnicodeAttribute(range_key=True)",
            updated=dt.datetime.now(tzutc()),
            # known at configuration
            vs30=350,  # vs30 value
            imt_codes=['PGA', 'SA(0.5)'],  # list of IMTs
            loc_codes=['WLG', 'AKL'],  # list of Location codes
            source_models=['A', 'B'],  # list of source model ids
            # extracted from the OQ HDF5
            source_df=json.dumps(dict(sources=[1, 2])),  # sources meta as DataFrame JSON
            gsim_df=json.dumps(dict(gsims=[1, 2])),  # gmpe meta as DataFrame JSON
            rlzs_df=json.dumps(dict(rlzs=[1, 2])),  # realization meta as DataFrame JSON
        )

        print(f'obj: {obj} {obj.version}')
        self.assertEqual(obj.version, None)

        obj.save()
        self.assertEqual(obj.version, 1)

        # self.assertEqual(obj.lvl_val_pairs[0].level, 0.001)
        # self.assertEqual(obj.lvl_val_pairs[0].value, 0.000001)
        # self.assertEqual(obj.lvl_val_pairs[9].level, 0.01)
        # self.assertEqual(obj.lvl_val_pairs[9].value, 0.00001)
