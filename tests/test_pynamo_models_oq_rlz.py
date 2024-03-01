import sqlite3

import pynamodb.exceptions
import pynamodb.models
import pynamodb.attributes
import toshi_hazard_store.model.openquake_models
import pytest
import json

class TestOpenquakeRealizationModel:
    def test_table_exists(self, adapted_rlz_model):
        assert adapted_rlz_model.OpenquakeRealization.exists()
        # self.assertEqual(model.ToshiOpenquakeMeta.exists(), True)


    def test_model_class(self, adapted_rlz_model, get_one_rlz):
        rlz = get_one_rlz()
        assert isinstance(rlz, pynamodb.models.Model)
        assert isinstance(rlz, toshi_hazard_store.model.openquake_models.OpenquakeRealization )

    @pytest.mark.skip('WIP: maybe belongs in db_adapter')
    def test_model_methods(self, adapted_rlz_model, get_one_rlz):
        rlz = get_one_rlz()
        # print(dir(rlz))
        # print( rlz.to_simple_dict(force=True))
        # print( rlz.to_dynamodb_dict())


        mRLZ = toshi_hazard_store.model.openquake_models.OpenquakeRealization

        row_dict = {}
        # simple_dict = rlz.to_simple_dict(force=True)
        for name, attr in mRLZ.get_attributes().items():
            if isinstance(attr, pynamodb.attributes.VersionAttribute):
                continue # these cannot be serialized yet

            print(dir(attr))

            # if mRLZ._range_key_attribute() and model_class._hash_key_attribute()

            # print(name, attr, getattr(rlz, name))
            json_str = json.dumps(attr.serialize(getattr(rlz, name)))
            row_dict[name] = json_str
            # print(attr.deserialize(json.loads(json_str)))

        print(row_dict)

        # print(mRLZ.created, dir(mRLZ.created))
        assert 0

    def from_sql(self):
        sql_row = {'agg': 'mean', 'created': 1709168888, 'hazard_model_id': 'MODEL_THE_FIRST', 'imt': 'PGA', 'lat': -36.87,
        'lon': 174.77, 'nloc_0': '-37.0~175.0', 'nloc_001': '-36.870~174.770', 'nloc_01': '-36.87~174.77', 'nloc_1': '-36.9~174.8',
        'partition_key': '-36.9~174.8', 'site_vs30': None, 'sort_key': '-36.870~174.770:250:PGA:mean:MODEL_THE_FIRST',
        'uniq_id': '056e5424-b5d6-48f8-89e7-2a54530a0303',
        'values': '''W3siTSI6IHsibHZsIjogeyJOIjogIjAuMDAxIn0sICJ2YWwiOiB7Ik4iOiAiMWUtMDYifX19LCB7Ik0iOiB7Imx2bCI6IHsiTiI6ICIwLjAwMiJ9LCAidmFsI
jogeyJOIjogIjJlLTA2In19fSwgeyJNIjogeyJsdmwiOiB7Ik4iOiAiMC4wMDMifSwgInZhbCI6IHsiTiI6ICIzZS0wNiJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDA0In0sICJ2YWwiOiB7Ik4iOiAiNGUtMDYifX19LCB7Ik0iOiB7Imx2bCI6IHsiTiI6ICIwLj
AwNSJ9LCAidmFsIjogeyJOIjogIjVlLTA2In19fSwgeyJNIjogeyJsdmwiOiB7Ik4iOiAiMC4wMDYifSwgInZhbCI6IHsiTiI6ICI2ZS0wNiJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDA3In0sICJ2YWwiOiB7Ik4iOiAiN2UtMDYifX19LCB7Ik0iOiB7Imx2bCI
6IHsiTiI6ICIwLjAwOCJ9LCAidmFsIjogeyJOIjogIjhlLTA2In19fSwgeyJNIjogeyJsdmwiOiB7Ik4iOiAiMC4wMDkifSwgInZhbCI6IHsiTiI6ICI5ZS0wNiJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDEifSwgInZhbCI6IHsiTiI6ICIxZS0wNSJ9fX0sIHsi
TSI6IHsibHZsIjogeyJOIjogIjAuMDExIn0sICJ2YWwiOiB7Ik4iOiAiMS4xZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDEyIn0sICJ2YWwiOiB7Ik4iOiAiMS4yZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDEzIn0sICJ2YWwiOiB7Ik4iO
iAiMS4zZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDE0In0sICJ2YWwiOiB7Ik4iOiAiMS40ZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDE1In0sICJ2YWwiOiB7Ik4iOiAiMS41ZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMD
E2In0sICJ2YWwiOiB7Ik4iOiAiMS42ZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDE3In0sICJ2YWwiOiB7Ik4iOiAiMS43ZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDE4In0sICJ2YWwiOiB7Ik4iOiAiMS44ZS0wNSJ9fX0sIHsiTSI6IHs
ibHZsIjogeyJOIjogIjAuMDE5In0sICJ2YWwiOiB7Ik4iOiAiMS45ZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDIifSwgInZhbCI6IHsiTiI6ICIyZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDIxIn0sICJ2YWwiOiB7Ik4iOiAiMi4xZS0w
NSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDIyIn0sICJ2YWwiOiB7Ik4iOiAiMi4yZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDIzIn0sICJ2YWwiOiB7Ik4iOiAiMi4zZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDI0In0sICJ2Y
WwiOiB7Ik4iOiAiMi40ZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDI1In0sICJ2YWwiOiB7Ik4iOiAiMi41ZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDI2In0sICJ2YWwiOiB7Ik4iOiAiMi42ZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogey
JOIjogIjAuMDI3In0sICJ2YWwiOiB7Ik4iOiAiMi43ZS0wNSJ9fX0sIHsiTSI6IHsibHZsIjogeyJOIjogIjAuMDI4In0sICJ2YWwiOiB7Ik4iOiAiMi44ZS0wNSJ9fX1d''',
        'version': 1,
        'vs30': 250
        }

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
