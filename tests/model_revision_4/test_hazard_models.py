"""
Basic model migration, structure
"""

from datetime import datetime, timezone
from moto import mock_dynamodb

from toshi_hazard_store.model import (
    CompatibleHazardCalculation,
    HazardCurveProducerConfig,
    HazardRealizationMeta,
    migrate_r4,
    drop_r4,
)


@mock_dynamodb
class TestRevisionFourModelCreation_PynamoDB:

    def test_tables_exists(self):
        migrate_r4()
        assert CompatibleHazardCalculation.exists()
        assert HazardCurveProducerConfig.exists()
        assert HazardRealizationMeta.exists()

        drop_r4()


class TestRevisionFourModelCreation_WithAdaption:

    def test_CompatibleHazardCalculation_table_exists(self, adapted_model):
        print(adapted_model.CompatibleHazardCalculation)
        assert adapted_model.CompatibleHazardCalculation.exists()

    def test_HazardCurveProducerConfig_table_exists(self, adapted_model):
        print(adapted_model.HazardCurveProducerConfig)
        assert adapted_model.HazardCurveProducerConfig.exists()

    def test_HazardRealizationMeta_table_exists(self, adapted_model):
        print(adapted_model.HazardRealizationMeta)
        assert adapted_model.HazardRealizationMeta.exists()

    def test_CompatibleHazardConfig_table_save_get(self, adapted_model):
        mCHC = adapted_model.CompatibleHazardCalculation
        m = mCHC(partition_key='A', uniq_id="AAA", notes='hello world')
        m.save()
        res = next(mCHC.query('A', mCHC.uniq_id == "AAA"))
        assert res.partition_key == "A"
        assert res.uniq_id == "AAA"
        assert res.notes == m.notes

    def test_HazardCurveProducerConfig_table_save_get(self, adapted_model):
        mHCPC = adapted_model.HazardCurveProducerConfig
        m = mHCPC(
            partition_key='A',
            range_key="openquake:3.16:#hashcode#",  # combination of the unique configuration identifiers
            compat_calc_fk="AAA",  # must map to a valid CompatibleHazardCalculation.uniq_id (maybe wrap in transaction)
            producer_software='openquake',
            producer_version_id='3.16',  # could also be a git rev
            configuration_hash='#hashcode#',
            configuration_data=None,
            notes='the original NSHM_v1.0.4 producer',
        )
        m.save()
        res = next(
            mHCPC.query(
                'A', mHCPC.range_key == "openquake:3.16:#hashcode#", mHCPC.compat_calc_fk == "AAA"  # filter_condition
            )
        )
        assert res.partition_key == "A"
        assert res.range_key == m.range_key
        assert res.notes == m.notes
        assert res.producer_software == m.producer_software

    def test_HazardRealizationMeta_table_save_get(self, adapted_model):
        mHRM = adapted_model.HazardRealizationMeta
        created = datetime(2020, 1, 1, 11, tzinfo=timezone.utc)
        m = mHRM(
            partition_key='A',
            range_key="HOW TO SET THIS??",  # how do we want to identify these (consider URIs as these are suitable for ANY setting)
            compat_calc_fk="AAA",  # must map to a valid CompatibleHazardCalculation.unique_id (maybe wrap in transaction)
            config_fk="openquake:3.16:#hashcode#",  # must map to a valid HazardCurveProducerConfig.unique_id (maybe wrap in transaction)
            created=created,
            vs30=999,  # vs30 value
        )
        m.save()
        res = next(
            mHRM.query(
                'A',
                mHRM.range_key == m.range_key,
                (mHRM.compat_calc_fk == "AAA")
                & (mHRM.config_fk == "openquake:3.16:#hashcode#")
                & (mHRM.vs30 == 999),  # filter_condition
            )
        )

        assert res.created == m.created
        assert res.vs30 == m.vs30
