"""Transform module depends on openquake and pandas."""

import sys
import unittest
from pathlib import Path

from moto import mock_dynamodb

from toshi_hazard_store import model

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


@mock_dynamodb
class TestMetaWithOpenquake(unittest.TestCase):
    def setUp(self):
        model.migrate()
        super(TestMetaWithOpenquake, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(TestMetaWithOpenquake, self).tearDown()

    @unittest.skip('this calc file needs later build of openquake: ValueError: Unknown GSIM: Atkinson2022SInter')
    @unittest.skipUnless(HAVE_OQ, "This test requires openquake")
    def test_export_meta_normalized_sitecode_on_disagg_hdf5(self):
        from openquake.calculators.export.hazard import get_sites
        from openquake.commonlib import datastore

        from toshi_hazard_store import oq_import

        TOSHI_ID = 'ABCBD'
        p = Path(Path(__file__).parent.parent, 'fixtures', 'disaggregation', 'calc_1.hdf5')
        dstore = datastore.read(str(p))

        sitemesh = get_sites(dstore['sitecol'])
        print('sitemesh', sitemesh)

        # do the saving....
        # oq_import.export_meta_v3(TOSHI_ID, dstore)
        oq_import.export_meta_v3(dstore, TOSHI_ID, "toshi_gt_id", "", ["source_tags"], ["source_ids"])
        # saved = list(model.ToshiOpenquakeHazardMeta.query(TOSHI_ID))
        saved = list(model.ToshiOpenquakeHazardMeta.scan())
        print('saved', saved)

        self.assertEqual(len(saved), 1)
        self.assertTrue('PGA' in saved[0].imts)
        self.assertIn("-35.220~173.970", saved[0].locs)
        print('saved', saved[0].locs)
