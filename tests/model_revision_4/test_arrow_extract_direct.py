import json
from pathlib import Path

import pytest
import uuid
import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
# import pandas as pd

from nzshm_common.location import coded_location

try:
    import openquake  # noqa
    HAVE_OQ = True
except ImportError:
    HAVE_OQ = False

if HAVE_OQ:
    from openquake.calculators.extract import Extractor

from toshi_hazard_store.oq_import.parse_oq_realizations import build_rlz_mapper

def disaggs_to_record_batch_reader(hdf5_file: str) -> pa.RecordBatchReader:
    """extract disagg statistics from from a 'disaggregation' openquake calc file as a pyarrow batch reader"""
    extractor = Extractor(str(hdf5_file))

    # oqparam contains the job specs, lots of different stuff for disaggs
    oqparam = json.loads(extractor.get('oqparam').json)

    assert oqparam['calculation_mode'] =='disaggregation', "calculation_mode is not 'disaggregation'"

    rlz_map = build_rlz_mapper(extractor)

    # ref https://github.com/gem/oq-engine/blob/75e96a90bbb88cd9ac0bb580a5283341c091b82b/openquake/calculators/extract.py#L1113
    #
    # different disagg kinds (from oq['disagg_outputs'])
    # e.g. ['TRT', 'Mag', 'Dist', 'Mag_Dist', 'TRT_Mag_Dist_Eps']
    da_trt = extractor.get('disagg?kind=TRT&imt=SA(0.5)&site_id=0&poe_id=0&spec=stats', asdict=True)
    da_mag = extractor.get('disagg?kind=Mag&imt=SA(0.5)&site_id=0&poe_id=0&spec=stats', asdict=True)
    da_dist = extractor.get('disagg?kind=Dist&imt=SA(0.5)&site_id=0&poe_id=0&spec=stats', asdict=True)
    da_mag_dist = extractor.get('disagg?kind=Mag_Dist&imt=SA(0.5)&site_id=0&poe_id=0&spec=stats', asdict=True)
    da_trt_mag_dist_eps = extractor.get('disagg?kind=TRT_Mag_Dist_Eps&imt=SA(0.5)&site_id=0&poe_id=0&spec=stats', asdict=True)

    '''
    >>> spec=stats
    >>> da_trt_mag_dist_eps['array'].shape
    (1, 24, 17, 16, 1, 1)
    >>> da_trt_mag_dist_eps.keys()
    dict_keys(['kind', 'imt', 'site_id', 'poe_id', 'spec', 'trt', 'mag', 'dist', 'eps', 'poe', 'traditional', 'shape_descr', 'extra', 'array'])
    '''

    '''
    >>> # STATS
    >>> da_trt = extractor.get('disagg?kind=TRT&imt=SA(0.5)&site_id=0&poe_id=0&spec=stats', asdict=True)
    >>> da_trt
    {'kind': ['TRT'], 'imt': ['SA(0.5)'], 'site_id': [0], 'poe_id': [0], 'spec': ['stats'], 'trt': array([b'Subduction Interface'], dtype='|S20'),
        'poe': array([9.99412581e-05]), 'traditional': False, 'shape_descr': ['trt', 'imt', 'poe'], 'extra': ['mean'],
        'array': array([[[9.99466419e-05]]])
    }

    >>> # RLZS
    >>> da_trt = extractor.get('disagg?kind=TRT&imt=SA(0.5)&site_id=0&poe_id=0&spec=rlzs', asdict=True)
    >>> da_trt
    {'kind': ['TRT'], 'imt': ['SA(0.5)'], 'site_id': [0], 'poe_id': [0], 'spec': ['rlzs'], 'trt':
        array([b'Subduction Interface'], dtype='|S20'), 'poe': array([9.99412581e-05]), 'traditional': False, 'shape_descr': ['trt', 'imt', 'poe'],
        'weights': [0.1080000102519989, 0.07200000435113907, 0.09600000828504562, 0.09600000828504562, 0.10000000894069672, 0.07500001043081284, 0.07200000435113907, 0.07200000435113907, 0.08100000768899918, 0.08100000768899918, 0.07200000435113907, 0.07500001043081284],
        'extra': ['rlz1', 'rlz9', 'rlz10', 'rlz7', 'rlz4', 'rlz3', 'rlz6', 'rlz11', 'rlz0', 'rlz2', 'rlz8', 'rlz5'],
        'array': array([[[7.27031471e-05, 1.40205725e-04, 6.89674751e-05, 4.83588026e-05,
             4.67680530e-05, 2.16860247e-04, 2.23101109e-04, 3.09774654e-05,
             3.68397989e-04, 8.67261109e-06, 6.76580881e-06, 6.21581990e-06]]])}
    >>>
    >>>
    '''


def rlzs_to_record_batch_reader(hdf5_file: str) -> pa.RecordBatchReader:
    """extract realizations from a 'classical' openquake calc file as a pyarrow batch reader"""
    extractor = Extractor(str(hdf5_file))
    oqparam = json.loads(extractor.get('oqparam').json)
    assert oqparam['calculation_mode'] =='classical', "calculation_mode is not 'classical'"

    rlz_map = build_rlz_mapper(extractor)
    #sites = extractor.get('sitecol').to_dframe()

    rlzs = extractor.get('hcurves?kind=rlzs', asdict=True)
    rlz_keys = [k for k in rlzs.keys() if 'rlz-' in k]

    #get the site properties
    site_location_props = {}
    for props in extractor.get('sitecol').to_dict()['array']:
        site_location_props[props[0]] = coded_location.CodedLocation(lat=props[2], lon=props[1], resolution=0.001).code

    #get the IMT props
    imtls = oqparam['hazard_imtls']  # dict of imt and the levels used at each imt e.g {'PGA': [0.011. 0.222]}

    # print(rlz_keys)
    # print('rlzs', rlzs[rlz_keys[0]])
    # print('shape', rlzs[rlz_keys[0]].shape)
    # print()

    def generate_rlz_record_batch(rlz_key:str, rlzs) -> pa.RecordBatch:

        a3d = rlzs[rlz_keys[0]] # 3D array for the given rlz_key
        n_sites, n_imts, n_values = a3d.shape

        #create the np.arrays for our three series
        values = a3d.reshape(n_sites*n_imts,n_values)
        site_idx = np.repeat(np.arange(n_sites),n_imts) # 0,0,0,0,0..........3991,3991
        imt_idx = np.tile(np.arange(n_imts), n_sites)   # 0,1,2,3.....0,1,2,3....26,27

        # build the site and imt series with DictionaryArrays (for effiency)
        # while imt values are kept in list form
        site_series = pa.DictionaryArray.from_arrays(site_idx, site_location_props.values())
        imt_series = pa.DictionaryArray.from_arrays(imt_idx, imtls.keys())
        values_series = values.tolist()

        batch = pa.RecordBatch.from_arrays([site_series, imt_series, values_series], ["nloc_001", "imt", "values"])
        return batch

    # create a schema...
    # TODO add all the fields: nloc_0, gmms_digest etc
    values_type = pa.list_(pa.float32())
    dict_type = pa.dictionary(pa.int32(), pa.string(), True)
    schema = pa.schema([("nloc_001", dict_type), ('imt', dict_type), ("values", values_type)])

    print('schema', schema)

    # an iterator for all the rlz batches
    def generate_rlz_batches(rlzs, rlz_keys):
        for rlz_key in rlz_keys:
            yield generate_rlz_record_batch(rlz_key, rlzs)

    record_batch_reader = pa.RecordBatchReader.from_batches(schema,
        generate_rlz_batches(rlzs, rlz_keys)
    )
    return record_batch_reader

@pytest.mark.skipif(not HAVE_OQ, reason="This test fails if openquake is not installed")
def test_hdf5_realisations_direct_to_parquet_rountrip(tmp_path):

    hdf5_fixture = Path(__file__).parent.parent / 'fixtures' / 'oq_import' / 'calc_1.hdf5'

    record_batch_reader = rlzs_to_record_batch_reader(str(hdf5_fixture))

    print(record_batch_reader)

    # now write out to parquet and validate
    output_folder = tmp_path / "ds_direct"

    # write the dataset
    dataset_format = 'parquet'
    ds.write_dataset(
        record_batch_reader,
        base_dir=output_folder,
        basename_template="%s-part-{i}.%s" % (uuid.uuid4(), dataset_format),
        # partitioning=['nloc_001'],
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
    assert df.shape == (1293084, 3)
