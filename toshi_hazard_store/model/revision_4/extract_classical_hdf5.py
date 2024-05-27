import json
import logging
from typing import Dict, List

import numpy as np
import pyarrow as pa

try:
    import openquake  # noqa

    HAVE_OQ = True
except ImportError:
    HAVE_OQ = False

if HAVE_OQ:
    from openquake.calculators.extract import Extractor

from nzshm_common.location import coded_location

from toshi_hazard_store.oq_import.parse_oq_realizations import build_rlz_mapper

log = logging.getLogger(__name__)


def build_nloc_0_mapping(nloc_001_locations: List[coded_location.CodedLocation]) -> Dict[str, int]:
    """a dictionary mapping CodedLocatoin.codes at res=1.0 to a unique integer index"""
    nloc_0_binned = coded_location.bin_locations(nloc_001_locations, at_resolution=1.0)
    nloc_0_map = {}
    for idx, coded_bin in enumerate(nloc_0_binned.keys()):
        nloc_0_map[coded_bin] = idx
    return nloc_0_map


def build_nloc0_series(nloc_001_locations: List[coded_location.CodedLocation], nloc_0_map: Dict[str, int]) -> List[int]:
    """return a new list with nloc_0 integer indices in place of the input arrays location indices

    this is used to populate the series data.
    """
    nloc_0_series = []
    for loc in nloc_001_locations:
        nloc_0_series.append(nloc_0_map[loc.downsample(1.0).code])
    return nloc_0_series


def rlzs_to_record_batch_reader(
    hdf5_file: str,
    calculation_id: str,
    compatible_calc_fk: str,
    producer_config_fk: str,  # TODO: decide if we actually want this column
) -> pa.RecordBatchReader:
    """extract realizations from a 'classical' openquake calc file as a pyarrow batch reader"""
    log.info(
        'rlzs_to_record_batch_reader called with '
        f'{hdf5_file}, {calculation_id}, {compatible_calc_fk}, {producer_config_fk}'
    )

    extractor = Extractor(str(hdf5_file))
    oqparam = json.loads(extractor.get('oqparam').json)
    assert oqparam['calculation_mode'] == 'classical', "calculation_mode is not 'classical'"

    vs30 = int(oqparam['reference_vs30_value'])

    # get the IMT props
    # imtls = oqparam['hazard_imtls']  # dict of imt and the levels used at each imt e.g {'PGA': [0.011. 0.222]}
    oq = extractor.dstore['oqparam']  # old skool way
    imtl_keys = sorted(list(oq.imtls.keys()))

    def generate_rlz_record_batches(extractor, vs30) -> pa.RecordBatch:

        rlzs = extractor.get('hcurves?kind=rlzs', asdict=True)
        rlz_keys = [k for k in rlzs.keys() if 'rlz-' in k]
        rlz_map = build_rlz_mapper(extractor)

        # get the site index values
        nloc_001_locations = []
        for props in extractor.get('sitecol').to_dict()['array']:
            site_loc = coded_location.CodedLocation(lat=props[2], lon=props[1], resolution=0.001)
            nloc_001_locations.append(site_loc)  # locations in OG order

        nloc_0_map = build_nloc_0_mapping(nloc_001_locations)
        nloc_0_series = build_nloc0_series(nloc_001_locations, nloc_0_map)

        # build the has digest dict arrays
        sources_digests = [r.sources.hash_digest for i, r in rlz_map.items()]
        gmms_digests = [r.gmms.hash_digest for i, r in rlz_map.items()]

        # iterate through all the rlzs, yielding the pyarrow record batches
        for r_idx, rlz_key in enumerate(rlz_keys):
            a3d = rlzs[rlz_key]  # 3D array for the given rlz_key

            n_sites, n_imts, n_values = a3d.shape

            # create the np.arrays for our series
            values = a3d.reshape(n_sites * n_imts, n_values)
            nloc_001_idx = np.repeat(np.arange(n_sites), n_imts)  # 0,0,0,0,0..........3991,3991
            nloc_0_idx = np.repeat(nloc_0_series, n_imts)  # 0,0.0,0,0..............56,56
            imt_idx = np.tile(np.arange(n_imts), n_sites)  # 0,1,2,3.....0,1,2,3....26,27
            rlz_idx = np.full(n_sites * n_imts, r_idx)  # 0..........................0
            vs30s_series = np.full(n_sites * n_imts, vs30)
            calculation_id_idx = np.full(n_sites * n_imts, 0)
            compatible_calc_idx = np.full(n_sites * n_imts, 0)
            producer_config_idx = np.full(n_sites * n_imts, 0)

            # Build the categorised series as pa.DictionaryArray objects
            compatible_calc_cat = pa.DictionaryArray.from_arrays(compatible_calc_idx, [compatible_calc_fk])
            producer_config_cat = pa.DictionaryArray.from_arrays(producer_config_idx, [producer_config_fk])
            calculation_id_cat = pa.DictionaryArray.from_arrays(calculation_id_idx, [calculation_id])
            nloc_001_cat = pa.DictionaryArray.from_arrays(nloc_001_idx, [loc.code for loc in nloc_001_locations])
            nloc_0_cat = pa.DictionaryArray.from_arrays(nloc_0_idx, nloc_0_map.keys())
            imt_cat = pa.DictionaryArray.from_arrays(imt_idx, imtl_keys)
            rlz_cat = pa.DictionaryArray.from_arrays(
                rlz_idx, rlz_keys
            )  # there's only one value in the dictionary on each rlz loop
            sources_digest_cat = pa.DictionaryArray.from_arrays(rlz_idx, sources_digests)
            gmms_digest_cat = pa.DictionaryArray.from_arrays(rlz_idx, gmms_digests)

            # while values are kept in list form
            values_series = values.tolist()
            batch = pa.RecordBatch.from_arrays(
                [
                    compatible_calc_cat,
                    producer_config_cat,
                    calculation_id_cat,
                    nloc_001_cat,
                    nloc_0_cat,
                    imt_cat,
                    vs30s_series,
                    rlz_cat,
                    sources_digest_cat,
                    gmms_digest_cat,
                    values_series,
                ],
                [
                    "compatible_calc_fk",
                    "producer_config_fk",
                    "calculation_id",
                    "nloc_001",
                    "nloc_0",
                    "imt",
                    "vs30",
                    "rlz",
                    "sources_digest",
                    "gmms_digest",
                    "values",
                ],
            )
            yield batch

    # create a schema...
    values_type = pa.list_(pa.float32())  # TODO CHECK if this is enough res, or float32 float64
    vs30_type = pa.int32()
    dict_type = pa.dictionary(pa.int32(), pa.string(), True)
    schema = pa.schema(
        [
            ("compatible_calc_fk", dict_type),  # id for hazard-calc equivalence, for PSHA engines interoperability
            # ("producer_config_fk", dict_type),    # id for the look up
            ("calculation_id", dict_type),  # a refernce to the original calculation that produced this item
            ("nloc_001", dict_type),  # the location string to three places e.g. "-38.330~17.550"
            ("nloc_0", dict_type),  # the location string to zero places e.g.  "-38.0~17.0" (used for partioning)
            ('imt', dict_type),  # the imt label e.g. 'PGA', 'SA(5.0)''
            ('vs30', vs30_type),  # the VS30 integer
            ('rlz', dict_type),  # the rlz id from the the original calculation
            ('sources_digest', dict_type),  # a unique hash id for the NSHM LTB source branch
            ('gmms_digest', dict_type),  # a unique hash id for the NSHM LTB gsim branch
            ("values", values_type),  # a list of the 44 IMTL values
        ]
    )

    # print('schema', schema)

    record_batch_reader = pa.RecordBatchReader.from_batches(schema, generate_rlz_record_batches(extractor, vs30))
    return record_batch_reader


# if __name__ == '__main__':

#     from toshi_hazard_store.model.revision_4 import pyarrow_dataset

#     WORKING = Path('/GNSDATA/LIB/toshi-hazard-store/WORKING')
#     GT_FOLDER = WORKING / "R2VuZXJhbFRhc2s6MTMyODQxNA=="
#     subtasks = GT_FOLDER / "subtasks"
#     assert subtasks.is_dir()

#     OUTPUT_FOLDER = WORKING / "ARROW" / "DIRECT_CLASSIC"

#     rlz_count = 0
#     for hdf5_file in subtasks.glob('**/*.hdf5'):
#         print(hdf5_file.parent.name)
#         model_generator = rlzs_to_record_batch_reader(
#             hdf5_file, calculation_id=hdf5_file.parent.name, compatible_calc_fk="A_A", producer_config_fk="A_B"
#         )
#         pyarrow_dataset.append_models_to_dataset(model_generator, OUTPUT_FOLDER)
#         # # log.info(f"Produced {model_count} source models from {subtask_info.hazard_calc_id} in {GT_FOLDER}")
#         lof.infi(f"processed all models in {hdf5_file.parent.name}")
#         break
