"""
Basic model migration, structure
"""

from moto import mock_dynamodb

from toshi_hazard_store.model import CompatibleHazardCalculation, migrate_r4, drop_r4


@mock_dynamodb
class TestRevisionFourModelCreation_PynamoDB:

    def test_CompatibleHazardConfig_table_exists(self):
        migrate_r4()
        assert CompatibleHazardCalculation.exists()
        drop_r4()


class TestRevisionFourModelCreation_WithAdaption:

    def test_CompatibleHazardConfig_table_exists(self, adapted_model):
        print(adapted_model.CompatibleHazardCalculation)
        assert adapted_model.CompatibleHazardCalculation.exists()

    def test_CompatibleHazardConfig_table_save_get(self, adapted_model):
        mCHC = adapted_model.CompatibleHazardCalculation
        m = mCHC(partition_key='A', uniq_id="AAA", notes='hello world')
        m.save()
        res = next(mCHC.query('A', mCHC.uniq_id == "AAA"))
        assert res.partition_key == "A"
        assert res.uniq_id == "AAA"
        assert res.notes == m.notes
