import json
import random
import unittest

from moto import mock_dynamodb
from nzshm_common.location.code_location import CodedLocation

from toshi_hazard_store import model


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
        vs30=0,
        site_vs30=random.randint(200, 1000),
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
        values=lvps,
        agg=model.AggregationEnum.MEAN.value,
        imt="PGA",
        vs30=0,
        site_vs30=random.randint(200, 1000),
        hazard_model_id="HAZ_MODEL_ONE",
    ).set_location(location)


def get_one_meta():
    return model.ToshiOpenquakeMeta(
        partition_key="ToshiOpenquakeMeta",
        hazard_solution_id="AMCDEF",
        general_task_id="GBBSGG",
        hazsol_vs30_rk="AMCDEF:350",
        # updated=dt.datetime.now(tzutc()),
        # known at configuration
        vs30=0,  # vs30 value
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
                rlz.partition_key, model.OpenquakeRealization.sort_key == '-41.300~174.780:000:000010:AMCDEF'
            )
        )[0]
        self.assertEqual(res.partition_key, rlz.partition_key)
        self.assertEqual(res.sort_key, rlz.sort_key)
        self.assertTrue(200 < res.site_vs30 < 1000)

        print(res.site_vs30)


@mock_dynamodb
class PynamoTestHazardAggregationQuery(unittest.TestCase):
    def setUp(self):

        model.migrate_openquake()
        super(PynamoTestHazardAggregationQuery, self).setUp()

    def tearDown(self):
        model.drop_openquake()
        return super(PynamoTestHazardAggregationQuery, self).tearDown()

    def test_model_query_equal_condition(self):

        hag = get_one_hazard_aggregate()
        hag.save()

        mHAG = model.HazardAggregation
        range_condition = mHAG.sort_key == '-41.300~174.780:000:PGA:mean:HAZ_MODEL_ONE'
        filter_condition = mHAG.vs30.is_in(0) & mHAG.imt.is_in('PGA') & mHAG.hazard_model_id.is_in('HAZ_MODEL_ONE')

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
        self.assertTrue(200 < res.site_vs30 < 1000)
