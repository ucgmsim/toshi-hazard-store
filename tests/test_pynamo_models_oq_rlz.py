import sqlite3

import pynamodb.attributes
import pynamodb.exceptions
import pynamodb.models
import pytest

import toshi_hazard_store.model.openquake_models


class TestOpenquakeRealizationModel:
    def test_table_exists(self, adapted_rlz_model):
        assert adapted_rlz_model.OpenquakeRealization.exists()
        # self.assertEqual(model.ToshiOpenquakeMeta.exists(), True)

    def test_model_class(self, adapted_rlz_model, get_one_rlz):
        rlz = get_one_rlz()
        assert isinstance(rlz, pynamodb.models.Model)
        assert isinstance(rlz, toshi_hazard_store.model.openquake_models.OpenquakeRealization)

    def test_save_one_new_realization_object(self, adapted_rlz_model, get_one_rlz):
        """New realization handles all the IMT levels."""
        print(adapted_rlz_model.__dict__['OpenquakeRealization'].__bases__)
        # with mock_dynamodb():
        # model.OpenquakeRealization.create_table(wait=True)
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


class TestOpenquakeRealizationQuery:
    def test_model_query_no_condition(self, adapted_rlz_model, get_one_rlz):
        rlz = get_one_rlz()
        rlz.save()

        # query on model
        res = list(
            adapted_rlz_model.OpenquakeRealization.query(
                rlz.partition_key, adapted_rlz_model.OpenquakeRealization.sort_key >= ""
            )
        )[0]
        assert res.partition_key == rlz.partition_key
        assert res.sort_key == rlz.sort_key

        assert rlz.values[0].lvls[0] == 1
        assert rlz.values[0].vals[0] == 101
        assert rlz.values[0].lvls[-1] == 50
        assert rlz.values[0].vals[-1] == 150

    def test_model_query_equal_condition(self, adapted_rlz_model, get_one_rlz):

        rlz = get_one_rlz()
        rlz.save()

        # query on model
        res = list(
            adapted_rlz_model.OpenquakeRealization.query(
                rlz.partition_key,
                adapted_rlz_model.OpenquakeRealization.sort_key == '-41.300~174.780:450:000010:AMCDEF',
            )
        )[0]

        assert res.partition_key == rlz.partition_key
        assert res.sort_key == rlz.sort_key

    @pytest.mark.skip("NO support in adapters for secondary indices.")
    def test_secondary_index_one_query(self, adapted_rlz_model, get_one_rlz):

        rlz = get_one_rlz()
        rlz.save()

        # query on model.index2
        res2 = list(
            adapted_rlz_model.OpenquakeRealization.index1.query(
                rlz.partition_key, adapted_rlz_model.OpenquakeRealization.index1_rk == "-41.3~174.8:450:000010:AMCDEF"
            )
        )[0]

        assert res2.partition_key == rlz.partition_key
        assert res2.sort_key == rlz.sort_key

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

    def test_save_duplicate_raises(self, adapted_rlz_model, get_one_rlz):
        """This relies on pynamodb version attribute on rlz models

        see https://pynamodb.readthedocs.io/en/stable/optimistic_locking.html#version-attribute
        """
        with pytest.raises((pynamodb.exceptions.PutError, sqlite3.IntegrityError)):
            rlza = get_one_rlz(adapted_rlz_model.OpenquakeRealization)
            rlza.save()
            rlzb = get_one_rlz(adapted_rlz_model.OpenquakeRealization)
            rlzb.save()

    # @pytest.maek.skip("not clear why pynamodb test fails with sqlite3 locked error")
    def test_batch_save_duplicate_wont_raise(self, adapted_rlz_model, get_one_rlz):
        """In Batch mode any duplicate keys will simply overwrite, that's the dynamodb way

        Because pynamodb version-checking needs conditional writes, and these are not supported in AWS batch operations.
        """
        # with pytest.raises((pynamodb.exceptions.PutError, sqlite3.IntegrityError)) as excinfo:
        rlza = get_one_rlz()
        rlzb = get_one_rlz()
        with adapted_rlz_model.OpenquakeRealization.batch_write() as batch:
            batch.save(rlzb)
            batch.save(rlza)

        # query on model
        res = list(
            adapted_rlz_model.OpenquakeRealization.query(
                rlza.partition_key,
                adapted_rlz_model.OpenquakeRealization.sort_key == '-41.300~174.780:450:000010:AMCDEF',
            )
        )
        assert len(res) == 1
