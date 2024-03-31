import json
from pathlib import Path

import pytest

try:
    import openquake  # noqa

    HAVE_OQ = True
except ImportError:
    HAVE_OQ = False

if HAVE_OQ:
    from openquake.calculators.extract import Extractor

    from toshi_hazard_store.oq_import import export_rlzs_rev4


@pytest.mark.skipif(not HAVE_OQ, reason="This test fails if openquake is not installed")
class TestOqImportRevisionFour:

    def test_CompatibleHazardCalculation_table_save_get(self, adapted_model):
        mCHC = adapted_model.CompatibleHazardCalculation
        m = mCHC(partition_key='A', uniq_id="AAA", notes='hello world')
        m.save()
        res = next(mCHC.query('A', mCHC.uniq_id == "AAA"))
        assert res.partition_key == "A"
        assert res.uniq_id == "AAA"
        assert res.notes == m.notes

    @pytest.mark.skip("mocking needed for odd sources in calc_9.hdf5")
    def test_export_rlzs_rev4(self, adapted_model):

        extractor = Extractor(str(Path(Path(__file__).parent.parent, 'fixtures/oq_import', 'calc_9.hdf5')))

        oq = json.loads(extractor.get('oqparam').json)
        imtls = oq['hazard_imtls']  # dict of imt and the levels used at each imt e.g {'PGA': [0.011. 0.222]}
        imts = list(imtls.keys())
        imt_levels = imtls[imts[0]]

        mCHC = adapted_model.CompatibleHazardCalculation
        compatible_calc = mCHC(partition_key='A', uniq_id="BB", notes='hello world')
        compatible_calc.save()

        mHCPC = adapted_model.HazardCurveProducerConfig
        producer_config = mHCPC(
            partition_key='CCC',
            range_key="openquake:3.16:#hashcode#",  # combination of the unique configuration identifiers
            compatible_calc_fk=compatible_calc.foreign_key(),
            # (
            #     "A",
            #     "BB",
            # ),  # must map to a valid CompatibleHazardCalculation.uniq_id (maybe wrap in transaction)
            producer_software='openquake',  # needs to be immutable ref and long-lived
            producer_version_id='3.16',  # could also be a git rev
            configuration_hash='#hashcode#',
            configuration_data=None,
            notes='the original NSHM_v1.0.4 producer',
            imts=imts,
            imt_levels=imt_levels,
        )
        producer_config.save()

        # Signature is different for rev4,
        rlzs = list(
            export_rlzs_rev4(
                extractor,
                compatible_calc=compatible_calc,
                producer_config=producer_config,
                # producer_config_fk=("CCC", "openquake:3.16:#hashcode#"),
                hazard_calc_id="ABC",
                vs30=400,
                # imts=m2.imts,
                # imt_levels=m2.imt_levels,
                return_rlz=True,
            )
        )

        assert rlzs[0].partition_key == '-41.3~174.8'
        assert (
            rlzs[0].sort_key == '-41.300~174.780:0400:PGA:A_BB:sa5ba3aeee1:gee0b5458f2'
        )  # -41.300~174.780:400:rlz-000:A_BB:CCC_openquake:3.16:#hashcode#'
        assert rlzs[0].calculation_id == "ABC"

        assert len(rlzs) == 64  # len(expected))
        assert len(rlzs[0].values) == 44
        assert rlzs[0].vs30 == 400  # expected[0].vs30)
        assert rlzs[0].imt == 'PGA'

        # self.assertEqual(rlzs[0].values[0].imt, expected[0].values[0].imt)
        # self.assertEqual(rlzs[0].values[0], expected[0].values[0])
        # self.assertEqual(rlzs[0].values[0].lvls, expected[0].values[0].lvls)

        # self.assertEqual(rlzs[0].rlz, expected[0].rlz)  # Pickle is out-of-whack

        # self.assertEqual(rlzs[0].hazard_solution_id, expected[0].hazard_solution_id)
        # self.assertEqual(rlzs[0].source_tags, expected[0].source_tags)
        # self.assertEqual(rlzs[0].source_ids, expected[0].source_ids)
