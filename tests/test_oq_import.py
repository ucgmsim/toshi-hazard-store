import pickle
import unittest
from pathlib import Path

from moto import mock_dynamodb

from toshi_hazard_store import model
from toshi_hazard_store.oq_import import export_meta_v3, export_rlzs_v3

try:
    import openquake  # noqa

    HAVE_OQ = True
except ImportError:
    HAVE_OQ = False


@mock_dynamodb
@unittest.skipUnless(HAVE_OQ, "This test fails if openquake is not installed")
class OqImportTest(unittest.TestCase):
    def setUp(self):

        from openquake.calculators.extract import Extractor

        self._hdf5_filepath = Path(Path(__file__).parent, 'fixtures/oq_import', 'calc_9.hdf5')
        self.meta_filepath = Path(Path(__file__).parent, 'fixtures/oq_import', 'meta')
        self.rlzs_filepath = Path(Path(__file__).parent, 'fixtures/oq_import', 'rlzs')
        self.extractor = Extractor(str(self._hdf5_filepath))
        # self.dframe = datastore.DataStore(str(self._hdf5_filepath))

        model.migrate()
        super(OqImportTest, self).setUp()

    def tearDown(self):
        model.drop_tables()
        return super(OqImportTest, self).tearDown()

    def test_export_meta(self):

        tags = ["TAG"]
        srcs = ["SRCA", "SRCB"]
        haz_id = "HAZID"
        gt_id = "GTID"
        loc_id = "NZ"

        meta = export_meta_v3(self.extractor, haz_id, gt_id, loc_id, tags, srcs)
        # meta = export_meta_v3(self.dframe, haz_id, gt_id, loc_id, tags, srcs)
        with open(self.meta_filepath, 'rb') as metafile:
            expected = pickle.load(metafile)

        meta.source_lt = meta.source_lt[expected.source_lt.columns]

        assert (meta.source_lt == expected.source_lt).all().all()
        assert (meta.gsim_lt == expected.gsim_lt).all().all()
        assert (meta.rlz_lt == expected.rlz_lt).all().all()

        self.assertEqual(meta.model.partition_key, expected.model.partition_key)
        self.assertEqual(meta.model.hazard_solution_id, meta.model.hazard_solution_id)
        self.assertEqual(meta.model.general_task_id, meta.model.general_task_id)
        self.assertEqual(meta.model.hazsol_vs30_rk, meta.model.hazsol_vs30_rk)
        self.assertEqual(meta.model.vs30, meta.model.vs30)
        self.assertEqual(meta.model.imts, meta.model.imts)
        self.assertEqual(meta.model.locations_id, meta.model.locations_id)
        self.assertEqual(meta.model.source_tags, meta.model.source_tags)
        self.assertEqual(meta.model.source_ids, meta.model.source_ids)
        self.assertEqual(meta.model.inv_time, meta.model.inv_time)

    def test_export_rlzs(self):

        with open(self.meta_filepath, 'rb') as metafile:
            meta = pickle.load(metafile)

        rlzs = list(export_rlzs_v3(self.extractor, meta, True))

        with open(self.rlzs_filepath, 'rb') as rlzsfile:
            expected = pickle.load(rlzsfile)

        self.assertEqual(len(rlzs), len(expected))
        self.assertEqual(len(rlzs[0].values), 1)

        self.assertEqual(rlzs[0].values[0].imt, expected[0].values[0].imt)
        self.assertEqual(rlzs[0].values[0].vals, expected[0].values[0].vals)
        self.assertEqual(rlzs[0].values[0].lvls, expected[0].values[0].lvls)

        self.assertEqual(rlzs[0].rlz, expected[0].rlz)
        self.assertEqual(rlzs[0].vs30, expected[0].vs30)
        self.assertEqual(rlzs[0].hazard_solution_id, expected[0].hazard_solution_id)
        self.assertEqual(rlzs[0].source_tags, expected[0].source_tags)
        self.assertEqual(rlzs[0].source_ids, expected[0].source_ids)
