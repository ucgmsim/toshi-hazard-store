import os
import unittest
from unittest import mock

import pynamodb.exceptions
import pytest
from moto import mock_dynamodb
from nzshm_common.location.code_location import CodedLocation
from pynamodb.models import Model

import toshi_hazard_store
from toshi_hazard_store import model
from toshi_hazard_store.v2.db_adapter import ensure_class_bases_begin_with
from toshi_hazard_store.v2.db_adapter.sqlite import SqliteAdapter


def get_one_hazard_aggregate():
    lvps = list(map(lambda x: model.LevelValuePairAttribute(lvl=x / 1e3, val=(x / 1e6)), range(1, 51)))
    location = CodedLocation(lat=-41.3, lon=174.78, resolution=0.001)
    return model.HazardAggregation(
        values=lvps, agg=model.AggregationEnum.MEAN.value, imt="PGA", vs30=450, hazard_model_id="HAZ_MODEL_ONE"
    ).set_location(location)


# ref https://docs.pytest.org/en/7.3.x/example/parametrize.html#deferring-the-setup-of-parametrized-resources
def pytest_generate_tests(metafunc):
    if "adapted_model" in metafunc.fixturenames:
        metafunc.parametrize("adapted_model", ["pynamodb", "sqlite"], indirect=True)


class TestOpenquakeRealizationModel:
    @pytest.mark.skip('fix base classes')
    def test_table_exists(self, adapted_model):
        assert model.OpenquakeRealization.exists()
        # self.assertEqual(model.ToshiOpenquakeMeta.exists(), True)

    @pytest.mark.skip('fix base classes')
    def test_save_one_new_realization_object(self, get_one_rlz, adapted_model):
        """New realization handles all the IMT levels."""
        print(model.__dict__['OpenquakeRealization'].__bases__)
        with mock_dynamodb():
            OpenquakeRealization.create_table(wait=True)
            rlz = get_one_rlz()
            # print(f'rlz: {rlz} {rlz.version}')
            rlz.save()
            # print(f'rlz: {rlz} {rlz.version}')
            # print(dir(rlz))
            assert rlz.values[0].lvls[0] == 1
            assert rlz.values[0].vals[0] == 101
            assert rlz.values[0].lvls[-1] == 50
            assert rlz.values[0].vals[-1] == 150
            assert rlz.partition_key == '-41.3~174.8'  # 0.1 degree res


"""
@pytest.mark.skip('fix base classes')
@mock_dynamodb
class PynamoTestOpenquakeRealizationQuery(unittest.TestCase):
    def setUp(self):

        model.migrate()
        super(PynamoTestOpenquakeRealizationQuery, self).setUp()

    def tearDown(self):
        model.drop_tables()
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
"""

"""
@mock_dynamodb
class PynamoTestHazardAggregationQuery(unittest.TestCase):
    def setUp(self):

        model.migrate_openquake()
        super(PynamoTestHazardAggregationQuery, self).setUp()

    def tearDown(self):
        model.drop_openquake()
        return super(PynamoTestHazardAggregationQuery, self).tearDown()

    def test_model_query_no_condition(self):

        hag = get_one_hazard_aggregate()
        hag.save()

        # query on model without range_key is not allowed
        with self.assertRaises(TypeError):
            list(model.HazardAggregation.query(hag.partition_key))[0]
            # self.assertEqual(res.partition_key, hag.partition_key)
            # self.assertEqual(res.sort_key, hag.sort_key)

    def test_model_query_equal_condition(self):

        hag = get_one_hazard_aggregate()
        hag.save()

        mHAG = model.HazardAggregation
        range_condition = mHAG.sort_key == '-41.300~174.780:450:PGA:mean:HAZ_MODEL_ONE'
        filter_condition = mHAG.vs30.is_in(450) & mHAG.imt.is_in('PGA') & mHAG.hazard_model_id.is_in('HAZ_MODEL_ONE')

        # query on model
        res = list(
            model.HazardAggregation.query(
                hag.partition_key,
                range_condition,
                filter_condition
                # model.HazardAggregation.sort_key == '-41.300~174.780:450:PGA:mean:HAZ_MODEL_ONE'
            )
        )[0]
        self.assertEqual(res.partition_key, hag.partition_key)
        self.assertEqual(res.sort_key, hag.sort_key)
"""
