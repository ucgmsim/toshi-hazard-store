"""
Basic model migration, structure
"""

# from datetime import datetime, timezone

from moto import mock_dynamodb

from toshi_hazard_store.model import drop_r4, migrate_r4


@mock_dynamodb
class TestRevisionFourModelCreation_PynamoDB:

    def test_tables_exists(self, adapted_model):
        migrate_r4()
        assert adapted_model.CompatibleHazardCalculation.exists()
        assert adapted_model.HazardCurveProducerConfig.exists()
        assert adapted_model.HazardRealizationCurve.exists()
        drop_r4()


class TestRevisionFourModelCreation_WithAdaption:

    def test_CompatibleHazardCalculation_table_exists(self, adapted_model):
        print(adapted_model.CompatibleHazardCalculation)
        assert adapted_model.CompatibleHazardCalculation.exists()

    def test_HazardCurveProducerConfig_table_exists(self, adapted_model):
        print(adapted_model.HazardCurveProducerConfig)
        assert adapted_model.HazardCurveProducerConfig.exists()

    def test_HazardRealizationCurve_table_exists(self, adapted_model):
        print(adapted_model.HazardRealizationCurve)
        assert adapted_model.HazardRealizationCurve.exists()

    def test_CompatibleHazardCalculation_table_save_get(self, adapted_model):
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
            compatible_calc_fk=(
                "A",
                "AA",
            ),  # must map to a valid CompatibleHazardCalculation.uniq_id (maybe wrap in transaction)
            producer_software='openquake',  # needs to be a long-lived, immutable ref
            producer_version_id='3.16',  # could also be a git rev
            configuration_hash='#hashcode#',
            configuration_data=None,
            notes='the original NSHM_v1.0.4 producer',
            imts=['PGA', 'SA(0.5)'],
            imt_levels=list(map(lambda x: x / 1e3, range(1, 51))),
        )
        m.save()
        assert m.version == 1

        res = next(
            mHCPC.query(
                'A',
                mHCPC.range_key == "openquake:3.16:#hashcode#",
                mHCPC.compatible_calc_fk == ("A", "AA"),  # filter_condition
            )
        )
        assert res.partition_key == "A"
        assert res.range_key == m.range_key
        assert res.notes == m.notes
        assert res.producer_software == m.producer_software
        assert res.version == 1

    def test_HazardRealizationCurve_table_save_get(self, adapted_model, generate_rev4_rlz_models):

        m = next(generate_rev4_rlz_models())
        print(m)
        mHRC = adapted_model.HazardRealizationCurve
        m.save()
        res = next(
            mHRC.query(
                m.partition_key,
                mHRC.sort_key == m.sort_key,
                # (mHRC.compatible_calc_fk == m.compatible_calc_fk)
                # & (mHRC.producer_config_fk == m.producer_config_fk)
                # & (mHRC.vs30 == m.vs30),  # filter_condition
            )
        )

        print(res)
        assert res.created.timestamp() == int(m.created.timestamp())  # approx
        assert res.vs30 == m.vs30
        assert res.imt == m.imt
        # assert res.values[0] == m.values[0]
        assert res.sort_key == '-38.160~178.247:0250:PGA:A_AA:sc9d8be924ee7:ga7d8c5d537e1'
        # assert res.sources_key() == 'c9d8be924ee7'
        # assert res.rlz == m.rlz TODO: need string coercion for sqladapter!
        # assert 0
