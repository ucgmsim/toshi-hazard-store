import json
from pathlib import Path

import pytest
import uuid
import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds

# import pandas as pd

from nzshm_common.location import coded_location
from nzshm_common.location import location

from typing import Dict, List, Optional

try:
    import openquake  # noqa

    HAVE_OQ = True
except ImportError:
    HAVE_OQ = False

if HAVE_OQ:
    from openquake.calculators.extract import Extractor

from toshi_hazard_store.oq_import.parse_oq_realizations import build_rlz_mapper
from toshi_hazard_store.transform import parse_logic_tree_branches
from toshi_hazard_store.oq_import.parse_oq_realizations import build_rlz_source_map, build_rlz_gmm_map
from toshi_hazard_store.oq_import.oq_manipulate_hdf5 import migrate_gsim_row, rewrite_calc_gsims

from toshi_hazard_store.model.revision_4 import extract_classical_hdf5


@pytest.mark.skip('showing my working')
def test_binning_locations():

    # from nzshm_common.location import coded_location
    good_file = Path(__file__).parent.parent / 'fixtures' / 'oq_import' / 'calc_1.hdf5'
    extractor = Extractor(str(good_file))

    nloc_001_locations = []
    for props in extractor.get('sitecol').to_dict()['array']:
        site_loc = coded_location.CodedLocation(lat=props[2], lon=props[1], resolution=0.001)
        nloc_001_locations.append(site_loc)  # locations in OG order

    nloc_0_map = extract_classical_hdf5.build_nloc_0_mapping(nloc_001_locations)
    print(nloc_0_map)
    nloc_0_series = extract_classical_hdf5.build_nloc0_series(nloc_001_locations, nloc_0_map)
    print(nloc_0_series)
    # nloc_0_dict = extract_classical_hdf5.build_nloc_0_dictionary(nloc_001_locations, nloc_0_map)
    # print(nloc_0_dict)

    assert 0

@pytest.mark.skip('large inputs not checked in')
def test_logic_tree_registry_lookup():

    good_file = Path(__file__).parent.parent / 'fixtures' / 'oq_import' / 'calc_1.hdf5'

    disagg = Path('/GNSDATA/LIB/toshi-hazard-store/WORKING/DISAGG')
    bad_file_1 = disagg / 'calc_1.hdf5'
    bad_file_2 = disagg / 'openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazoxMDYzMzU3' / 'calc_1.hdf5'
    bad_file_3 = disagg / 'openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazo2OTI2MTg2' / 'calc_1.hdf5'
    bad_file_4 = disagg / 'openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazoxMzU5MTQ1' / 'calc_1.hdf5'

    # rewrite_calc_gsims(bad_file_4)
    # assert 0

    def build_maps(hdf5_file):
        extractor = Extractor(str(hdf5_file))
        # oqparam = json.loads(extractor.get('oqparam').json)
        source_lt, gsim_lt, rlz_lt = parse_logic_tree_branches(extractor)

        # check gsims
        gmm_map = build_rlz_gmm_map(gsim_lt)
        # check sources
        try:
            src_map = build_rlz_source_map(source_lt)
        except KeyError as exc:
            print(exc)
            raise
            # return False
        return True

    assert build_maps(good_file)

    # first subtask of first gt in gt_index
    # >>> ValueError: Unknown GSIM: ParkerEtAl2021SInter
    # T3BlbnF1YWtlSGF6YXJkVGFzazoxMzU5MTQ1 from  R2VuZXJhbFRhc2s6MTM1OTEyNQ==
    #
    # Created: April 3rd, 2023 at 3:42:21 PM GMT+12
    # Description: hazard ID: NSHM_v1.0.4, hazard aggregation target: mean
    #
    # raises KeyError: 'disaggregation sources'

    """
    >>> gt_index['R2VuZXJhbFRhc2s6MTM1OTEyNQ==']['arguments']
        {'hazard_config': 'RmlsZToxMjkxNjk4', 'model_type': 'COMPOSITE', 'disagg_config':
        "{'source_ids': ['SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyOTE2MTE=', 'RmlsZToxMzA3MzI='], 'nrlz': 12, 'location': '-39.500~176.900',
        'site_name': None, 'site_code': None, 'vs30': 300, 'imt': 'PGA', 'poe': 0.02, 'inv_time': 50,
        'target_level': 1.279633045964304, 'level': 1.279633045964304,
        'disagg_settings': {'disagg_bin_edges': {'dist': [0, 5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0, 60.0, 80.0, 100.0, 140.0, 180.0, 220.0, 260.0, 320.0, 380.0, 500.0]},
        'num_epsilon_bins': 16, 'mag_bin_width': 0.1999, 'coordinate_bin_width': 5, 'disagg_outputs': 'TRT Mag Dist Mag_Dist TRT_Mag_Dist_Eps'}}",
        'hazard_model_id': 'NSHM_v1.0.4', 'hazard_agg_target': 'mean', 'rupture_mesh_spacing': '4', 'ps_grid_spacing': '30', 'vs30': '300',
        logic_tree_permutations': "[{'permute': [{'members': [{'tag': 'DISAGG', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyOTE2MTE=', 'bg_id': 'RmlsZToxMzA3MzI=', 'weight': 1.0}]}]}]"}

    """
    assert not build_maps(bad_file_4), f"bad_file_4 build map fails"

    # first subtask of last gt in gt_index
    # T3BlbnF1YWtlSGF6YXJkVGFzazo2OTI2MTg2 from R2VuZXJhbFRhc2s6NjkwMTk2Mw==
    #
    # Created: March 22nd, 2024 at 11:51:20 AM GMT+13
    # Description: Disaggregation NSHM_v1.0.4
    #
    # raises KeyError: '[dm0.7, bN[0.902, 4.6], C4.0, s0.28]'

    """
    >>> args = gt_index['R2VuZXJhbFRhc2s6NjkwMTk2Mw==']['arguments']

    """
    assert not build_maps(bad_file_3), f"bad_file_3 build map fails"

    # 2nd random choice (weird setup) ++ ValueError: Unknown GSIM: ParkerEtAl2021SInter
    # T3BlbnF1YWtlSGF6YXJkVGFzazoxMDYzMzU3 from ??
    # Created: February 2nd, 2023 at 9:22:36 AM GMT+13
    # raises KeyError: 'disaggregation sources'

    assert not build_maps(bad_file_2), f"bad_file_2 build map fails"

    # first random choice
    # raises KeyError: '[dmTL, bN[0.95, 16.5], C4.0, s0.42]'
    assert not build_maps(bad_file_1), f"bad_file_1 build map fails"


@pytest.mark.skipif(not HAVE_OQ, reason="This test fails if openquake is not installed")
def test_hdf5_realisations_direct_to_parquet_roundtrip(tmp_path):

    hdf5_fixture = Path(__file__).parent.parent / 'fixtures' / 'oq_import' / 'calc_1.hdf5'

    record_batch_reader = extract_classical_hdf5.rlzs_to_record_batch_reader(str(hdf5_fixture),
        calculation_id = "dummy_calc_id",
        compatible_calc_fk = "CCFK",
        producer_config_fk = "PCFK")

    print(record_batch_reader)

    # now write out to parquet and validate
    output_folder = tmp_path / "ds_direct"

    # write the dataset
    dataset_format = 'parquet'
    ds.write_dataset(
        record_batch_reader,
        base_dir=output_folder,
        basename_template="%s-part-{i}.%s" % (uuid.uuid4(), dataset_format),
        partitioning=['nloc_0'],
        partitioning_flavor="hive",
        existing_data_behavior="overwrite_or_ignore",
        format=dataset_format,
    )

    # read and check the dataset
    dataset = ds.dataset(output_folder, format='parquet', partitioning='hive')
    table = dataset.to_table()
    df = table.to_pandas()

    # assert table.shape[0] == model_count
    # assert df.shape[0] == model_count
    print(df)
    print(df.shape)
    print(df.tail())
    print(df.info())
    assert df.shape == (1293084, 10)

    test_loc = location.get_locations(['MRO'])[0]

    test_loc_df = df[df['nloc_001'] == test_loc.code]
    print(test_loc_df[['nloc_001', 'nloc_0', 'imt', 'rlz', 'vs30', 'sources_digest', 'gmms_digest']])  # 'rlz_key'
    # print(test_loc_df.tail())

    assert test_loc_df.shape == (1293084 / 3991, 10)
    assert test_loc_df['imt'].tolist()[0] == 'PGA'
    assert test_loc_df['imt'].tolist()[-1] == 'SA(7.5)', "not so weird, as the IMT keys are sorted alphnumerically in openquake now."
    assert test_loc_df['imt'].tolist().index('SA(10.0)') == 17 , "also not so weird, as the IMT keys are sorted alphnumerically"

    assert test_loc_df['nloc_001'].tolist()[0] == test_loc.code
    assert test_loc_df['nloc_0'].tolist()[0] == test_loc.resample(1.0).code
