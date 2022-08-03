import json
import unittest

import pynamodb.exceptions
from moto import mock_dynamodb
from nzshm_common.location.code_location import CodedLocation

from toshi_hazard_store import model

# from toshi_hazard_store.model.openquake_v1_model import LevelValuePairAttribute


def get_one_rlz():
    imtvs = []
    for t in ['PGA', 'SA(0.5)', 'SA(1.0)']:
        levels = range(1, 51)
        values = range(101, 151)
        imtvs.append(model.IMTValuesAttribute(imt="PGA", lvls=levels, vals=values))

    location = CodedLocation(lat=-41.3, lon=174.78, resolution=0.001)
    rlz = model.OpenquakeRealization(
        values=imtvs,
        rlz=10,
        vs30=450,
        hazard_solution_id="AMCDEF",
        source_tags=["hiktlck", "b0.979", "C3.9", "s0.78"],
        source_ids=["SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwODA3NQ==", "RmlsZToxMDY1MjU="],
    )
    rlz.set_location(location)
    return rlz


def get_one_hazard_aggregate():
    lvps = list(map(lambda x: model.LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, 51)))
    location = CodedLocation(lat=-41.3, lon=174.78, resolution=0.001)
    return model.HazardAggregation(
        values=lvps, agg="mean", imt="PGA", vs30=450, hazard_model_id="HAZ_MODEL_ONE"
    ).set_location(location)


def get_one_meta():
    return model.ToshiOpenquakeMeta(
        partition_key="ToshiOpenquakeMeta",
        hazard_solution_id="AMCDEF",
        general_task_id="GBBSGG",
        hazsol_vs30_rk="AMCDEF:350",
        # updated=dt.datetime.now(tzutc()),
        # known at configuration
        vs30=350,  # vs30 value
        imts=['PGA', 'SA(0.5)'],  # list of IMTs
        locations_id='AKL',  # Location code or list ID
        source_tags=["hiktlck", "b0.979", "C3.9", "s0.78"],
        source_ids=["SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwODA3NQ==", "RmlsZToxMDY1MjU="],
        inv_time=1.0,
        # extracted from the OQ HDF5
        src_lt=json.dumps(dict(sources=[1, 2])),  # sources meta as DataFrame JSON
        gsim_lt=json.dumps(dict(gsims=[1, 2])),  # gmpe meta as DataFrame JSON
        rlz_lt=json.dumps(dict(rlzs=[1, 2])),  # realization meta as DataFrame JSON
    )


@mock_dynamodb
class PynamoTestMeta(unittest.TestCase):
    def setUp(self):

        model.migrate_v3()
        super(PynamoTestMeta, self).setUp()

    def tearDown(self):
        model.drop_tables_v3()
        return super(PynamoTestMeta, self).tearDown()

    def test_table_exists(self):
        self.assertEqual(model.OpenquakeRealization.exists(), True)
        self.assertEqual(model.ToshiOpenquakeMeta.exists(), True)

    def test_save_one_meta_object(self):
        obj = get_one_meta()

        print(f'obj: {obj} {obj.version}')
        self.assertEqual(obj.version, None)
        obj.save()
        self.assertEqual(obj.version, 1)

    def test_save_duplicate_raises(self):
        meta_a = get_one_meta()
        meta_a.save()

        meta_b = get_one_meta()
        with self.assertRaises(pynamodb.exceptions.PutError):
            meta_b.save()


@mock_dynamodb
class PynamoTestTwo(unittest.TestCase):
    def setUp(self):

        model.migrate_v3()
        super(PynamoTestTwo, self).setUp()

    def tearDown(self):
        model.drop_tables_v3()
        return super(PynamoTestTwo, self).tearDown()

    def test_table_exists(self):
        self.assertEqual(model.OpenquakeRealization.exists(), True)
        self.assertEqual(model.ToshiOpenquakeMeta.exists(), True)

    def test_save_one_new_realization_object(self):
        """New realization handles all the IMT levels."""
        rlz = get_one_rlz()

        # print(f'rlz: {rlz} {rlz.version}')
        rlz.save()
        # print(f'rlz: {rlz} {rlz.version}')
        # print(dir(rlz))

        self.assertEqual(rlz.values[0].lvls[0], 1)
        self.assertEqual(rlz.values[0].vals[0], 101)
        self.assertEqual(rlz.values[0].lvls[-1], 50)
        self.assertEqual(rlz.values[0].vals[-1], 150)

        self.assertEqual(rlz.partition_key, '-41.3~174.8')  # 0.1 degree res


@mock_dynamodb
class PynamoTestOpenquakeRealizationQuery(unittest.TestCase):
    def setUp(self):

        model.migrate_v3()
        super(PynamoTestOpenquakeRealizationQuery, self).setUp()

    def tearDown(self):
        model.drop_tables_v3()
        return super(PynamoTestOpenquakeRealizationQuery, self).tearDown()

    def test_model_query_no_condition(self):

        rlz = get_one_rlz()
        rlz.save()

        # query on model
        res = list(model.OpenquakeRealization.query(rlz.partition_key))[0]
        self.assertEqual(res.partition_key, rlz.partition_key)
        self.assertEqual(res.sort_key, rlz.sort_key)

    def test_model_query_equal_condition(self):

        rlz = get_one_rlz()
        rlz.save()

        # query on model
        res = list(
            model.OpenquakeRealization.query(
                rlz.partition_key, model.OpenquakeRealization.sort_key == '-41.300~174.780:450:000010:AMCDEF'
            )
        )[0]
        self.assertEqual(res.partition_key, rlz.partition_key)
        self.assertEqual(res.sort_key, rlz.sort_key)

    def test_secondary_index_one_query(self):

        rlz = get_one_rlz()
        rlz.save()

        # query on model.index2
        res2 = list(
            model.OpenquakeRealization.index1.query(
                rlz.partition_key, model.OpenquakeRealization.index1_rk == "-41.3~174.8:450:000010:AMCDEF"
            )
        )[0]

        self.assertEqual(res2.partition_key, rlz.partition_key)
        self.assertEqual(res2.sort_key, rlz.sort_key)

    # def test_secondary_index_two_query(self):

    #     rlz = get_one_rlz()
    #     rlz.save()

    #     # query on model.index2
    #     res2 = list(
    #         model.OpenquakeRealization.index2.query(
    #             rlz.partition_key, model.OpenquakeRealization.index2_rk == "450:-41.300~174.780:05000000:000010"
    #         )
    #     )[0]

    #     self.assertEqual(res2.partition_key, rlz.partition_key)
    #     self.assertEqual(res2.sort_key, rlz.sort_key)

    def test_save_duplicate_raises(self):

        rlza = get_one_rlz()
        rlza.save()

        rlzb = get_one_rlz()
        with self.assertRaises(pynamodb.exceptions.PutError):
            rlzb.save()

    @unittest.skip("This test is invalid")
    def test_batch_save_duplicate_raises(self):

        rlza = get_one_rlz()
        with model.OpenquakeRealization.batch_write() as batch:
            batch.save(rlza)

        with self.assertRaises(pynamodb.exceptions.PutError):
            rlzb = get_one_rlz()
            with model.OpenquakeRealization.batch_write() as batch:
                batch.save(rlzb)

    @unittest.skip("And this test is invalid")
    def test_batch_save_internal_duplicate_raises(self):
        with self.assertRaises(pynamodb.exceptions.PutError):
            rlza = get_one_rlz()
            rlzb = get_one_rlz()
            with model.OpenquakeRealization.batch_write() as batch:
                batch.save(rlzb)
                batch.save(rlza)


@mock_dynamodb
class PynamoTestHazardAggregationQuery(unittest.TestCase):
    def setUp(self):

        model.migrate_v3()
        super(PynamoTestHazardAggregationQuery, self).setUp()

    def tearDown(self):
        model.drop_tables_v3()
        return super(PynamoTestHazardAggregationQuery, self).tearDown()

    def test_model_query_no_condition(self):

        hag = get_one_hazard_aggregate()
        hag.save()

        # query on model
        res = list(model.HazardAggregation.query(hag.partition_key))[0]
        self.assertEqual(res.partition_key, hag.partition_key)
        self.assertEqual(res.sort_key, hag.sort_key)

    def test_model_query_equal_condition(self):

        hag = get_one_hazard_aggregate()
        hag.save()

        # query on model
        res = list(
            model.HazardAggregation.query(
                hag.partition_key, model.HazardAggregation.sort_key == '-41.300~174.780:450:PGA:mean:HAZ_MODEL_ONE'
            )
        )[0]
        self.assertEqual(res.partition_key, hag.partition_key)
        self.assertEqual(res.sort_key, hag.sort_key)
