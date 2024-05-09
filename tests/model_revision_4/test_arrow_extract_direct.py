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
    # from toshi_hazard_store.oq_import import export_rlzs_rev4

from toshi_hazard_store.oq_import.parse_oq_realizations import build_rlz_mapper
# from toshi_hazard_store.oq_import.oq_manipulate_hdf5 import rewrite_calc_gsims
# from toshi_hazard_store.model.revision_4 import pyarrow_dataset

def rlzs_to_record_batch_reader(hdf5_file: str):
    extractor = Extractor(str(hdf5_file))
    rlz_map = build_rlz_mapper(extractor)

    oq = json.loads(extractor.get('oqparam').json)
    #sites = extractor.get('sitecol').to_dframe()

    rlzs = extractor.get('hcurves?kind=rlzs', asdict=True)
    rlz_keys = [k for k in rlzs.keys() if 'rlz-' in k]

    #get the site properties
    site_location_props = {}
    for props in extractor.get('sitecol').to_dict()['array']:
        site_location_props[props[0]] = coded_location.CodedLocation(lat=props[2], lon=props[1], resolution=0.001).code

    #get the IMT props
    imtls = oq['hazard_imtls']  # dict of imt and the levels used at each imt e.g {'PGA': [0.011. 0.222]}

    # print(rlz_keys)
    # print('rlzs', rlzs[rlz_keys[0]])
    # print('shape', rlzs[rlz_keys[0]].shape)
    # print()

    def generate_rlz_record_batch(rlz_key:str, rlzs):
        a = rlzs[rlz_keys[0]] # 3D array for the given rlz_key
        m,n,r = a.shape

        #create the np.arrays for our three table series
        values = a.reshape(m*n,-1)
        site_idx = np.repeat(np.arange(m),n)
        imt_idx = np.tile(np.arange(n), m)

        # build the series with DictionaryArrays for site and imt
        # while imtl values are kept in list form
        site_series = pa.DictionaryArray.from_arrays(site_idx, site_location_props.values())
        imt_series = pa.DictionaryArray.from_arrays(imt_idx, imtls.keys())
        values_series = values.tolist()

        # the record batch can be pass to Dataset just like a table
        batch = pa.RecordBatch.from_arrays([site_series, imt_series, values_series], ["nloc_001", "imt", "values"])
        return batch

    #create a schema...
    # TODO add all the fields: nloc_0, gmms_digest etc
    values_type = pa.list_(pa.float32())
    dict_type = pa.dictionary(pa.int32(), pa.string(), True)
    schema = pa.schema([("nloc_001", dict_type), ('imt', dict_type), ("values", values_type)])

    print('schema', schema)

    #create an iterator for all the rlz batches
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
