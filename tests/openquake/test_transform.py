"""Transform module depends on openquake and pandas."""

import sys
import unittest
from pathlib import Path

try:
    import openquake  # noqa

    HAVE_OQ = True
except ImportError:
    HAVE_OQ = False

HAVE_MOCK_SERVER = False  # todo set up the moto mock_server properly


class TestWithoutOpenquake(unittest.TestCase):
    """This test class disables openquake for testing, even if it's actually installed."""

    def setUp(self):
        self._temp_oq = None
        if sys.modules.get('openquake'):
            self._temp_oq = sys.modules['openquake']
        sys.modules['openquake'] = None

    def tearDown(self):
        if self._temp_oq:
            sys.modules['openquake'] = self._temp_oq
        else:
            del sys.modules['openquake']

    def test_there_will_be_no_openquake_even_if_installed(self):
        flag = False
        try:
            import openquake  # noqa
        except ImportError:
            flag = True
        self.assertTrue(flag)

    @unittest.skipUnless(not HAVE_OQ, "This test fails if openquake is installed")
    def test_no_openquake_raises_import_error_on_transform_modules(self):
        flag = False
        try:
            import toshi_hazard_store.transform  # noqa
        except (ModuleNotFoundError, ImportError):
            flag = True
        self.assertTrue(flag)


class TestMetaWithOpenquake:

    def test_export_meta_normalized_sitecode_on_disagg_hdf5(self, adapted_meta_model):
        # from openquake.calculators.export.hazard import get_sites
        import openquake.engine as oq_engine
        from openquake.calculators.extract import Extractor

        # from openquake.commonlib import datastore
        from toshi_hazard_store import oq_import

        assert oq_engine.__version__ == '3.19.0'  # need devel==3.19 to get the extra NSHM GMMs

        TOSHI_ID = 'ABCBD'
        # p = Path(Path(__file__).parent.parent, 'fixtures', 'disaggregation', 'calc_1.hdf5')
        p = Path(Path(__file__).parent.parent, 'fixtures', 'oq_import', 'calc_9.hdf5')

        # dstore = datastore.read(str(p))
        extractor = Extractor(str(p))

        # sitemesh = get_sites(extractor.get('sitecol'))
        # print('sitemesh', sitemesh)

        # do the saving....
        # oq_import.export_meta_v3(TOSHI_ID, dstore)
        oq_import.export_meta_v3(extractor, TOSHI_ID, "toshi_gt_id", "", ["source_tags"], ["source_ids"])
        # saved = list(model.ToshiOpenquakeMeta.query(TOSHI_ID))
        # saved = list(adapted_meta_model.ToshiOpenquakeMeta.scan())
        saved = list(
            adapted_meta_model.ToshiOpenquakeMeta.query(
                "ToshiOpenquakeMeta", adapted_meta_model.ToshiOpenquakeMeta.hazsol_vs30_rk == f"{TOSHI_ID}:400"
            )
        )

        print('saved', saved)

        assert len(saved) == 1
        assert 'PGA' in saved[0].imts
        # self.assertIn("-35.220~173.970", saved[0].locs)
        # print('saved', saved[0].locs)
