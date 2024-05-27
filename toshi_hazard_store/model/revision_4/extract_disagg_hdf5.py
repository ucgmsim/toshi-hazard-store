# flake8: noqa
import json
import logging
import pathlib
import uuid
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pytest

try:
    import openquake  # noqa

    HAVE_OQ = True
except ImportError:
    HAVE_OQ = False

if HAVE_OQ:
    from openquake.calculators.extract import Extractor

from nzshm_common.location import coded_location, location

from toshi_hazard_store.model.revision_4 import pyarrow_dataset
from toshi_hazard_store.model.revision_4.extract_classical_hdf5 import build_nloc0_series, build_nloc_0_mapping
from toshi_hazard_store.oq_import.parse_oq_realizations import build_rlz_mapper

logging.basicConfig(level=logging.DEBUG)
log = logging.getLogger(__name__)
# log.setLevel(logging.DEBUG)


def disaggs_to_record_batch_reader(
    hdf5_file: pathlib.Path, calculation_id: str, compatible_calc_fk: str, producer_config_fk: str
) -> pa.RecordBatchReader:
    """extract disagg statistics from from a 'disaggregation' openquake calc file as a pyarrow batch reader"""
    extractor = Extractor(str(hdf5_file))

    # oqparam contains the job specs, lots of different stuff for disaggs
    oqparam = json.loads(extractor.get('oqparam').json)

    assert oqparam['calculation_mode'] == 'disaggregation', "calculation_mode is not 'disaggregation'"

    vs30 = int(oqparam['reference_vs30_value'])

    print(oqparam)

    imts = list(oqparam['iml_disagg'].keys())

    # get the site index values
    nloc_001_locations = []
    for props in extractor.get('sitecol').to_dict()['array']:
        site_loc = coded_location.CodedLocation(lat=props[2], lon=props[1], resolution=0.001)
        nloc_001_locations.append(site_loc)  # locations in OG order

    nloc_0_map = build_nloc_0_mapping(nloc_001_locations)
    nloc_0_series = build_nloc0_series(nloc_001_locations, nloc_0_map)

    # print(nloc_001_locations)
    # print(nloc_0_map)

    # TODO decide on approach to source branch identification
    # rlz_map = build_rlz_mapper(extractor)

    # ref https://github.com/gem/oq-engine/blob/75e96a90bbb88cd9ac0bb580a5283341c091b82b/openquake/calculators/extract.py#L1113
    #
    # different disagg kinds (from oqparam['disagg_outputs'])
    # e.g. ['TRT', 'Mag', 'Dist', 'Mag_Dist', 'TRT_Mag_Dist_Eps']
    # da_trt = extractor.get('disagg?kind=TRT&imt=SA(0.5)&site_id=0&poe_id=0&spec=stats', asdict=True)
    # da_mag = extractor.get('disagg?kind=Mag&imt=SA(0.5)&site_id=0&poe_id=0&spec=stats', asdict=True)
    # da_dist = extractor.get('disagg?kind=Dist&imt=SA(0.5)&site_id=0&poe_id=0&spec=stats', asdict=True)
    # da_mag_dist = extractor.get('disagg?kind=Mag_Dist&imt=SA(0.5)&site_id=0&poe_id=0&spec=stats', asdict=True)
    disagg_rlzs = extractor.get(
        f'disagg?kind=TRT_Mag_Dist_Eps&imt={imts[0]}&site_id=0&poe_id=0&spec=rlzs',
        # asdict=True
    )

    def build_batch(disagg_rlzs, nloc_0: int, nloc_001: int):

        print('kind', disagg_rlzs.kind)
        print('imt', disagg_rlzs.imt)
        print('site_id', disagg_rlzs.site_id)

        # PROBLEM trt array is empty!!
        # in this example  hdf5_file = WORKING / 'openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazoxMzU5MTQ1' / 'calc_1.hdf5'
        # print('trt dir', dir(disagg_rlzs.trt))
        # print('trt type', type(disagg_rlzs.trt))
        # print('trt shape', disagg_rlzs.trt.shape)
        trt_values = disagg_rlzs.trt.tolist()
        # print('trt_values', trt_values)

        if not trt_values:
            trt_values = ['TRT unknown']

        # TODO: build the hash digest dict arrays
        # sources_digests = [r.sources.hash_digest for i, r in rlz_map.items()]
        # gmms_digests = [r.gmms.hash_digest for i, r in rlz_map.items()]

        # Now we must convert the n_dimensional mumpy array into columnar series
        # shape_descr ['trt', 'mag', 'dist', 'eps', 'imt', 'poe']
        nested_array = disagg_rlzs.array  # 3D array for the given rlz_key
        n_trt, n_mag, n_dist, n_eps, n_imt, n_poe = nested_array.shape
        log.debug(f'shape {nested_array.shape}')
        all_indices = n_trt * n_mag * n_dist * n_eps * n_imt * n_poe

        assert len(disagg_rlzs.extra) == n_poe

        # create the np.arrays for our series
        trt_idx = np.repeat(np.arange(n_trt), all_indices / n_trt)
        mag_idx = np.repeat(np.tile(np.arange(n_mag), n_trt), all_indices / (n_trt * n_mag))
        dist_idx = np.repeat(np.tile(np.arange(n_dist), (n_trt * n_mag)), all_indices / (n_trt * n_mag * n_dist))
        eps_idx = np.repeat(
            np.tile(np.arange(n_eps), (n_trt * n_mag * n_dist)), all_indices / (n_trt * n_mag * n_dist * n_eps)
        )
        imt_idx = np.repeat(
            np.tile(np.arange(n_imt), (n_trt * n_mag * n_dist * n_eps)),
            all_indices / (n_trt * n_mag * n_dist * n_eps * n_imt),
        )

        rlz_idx = np.tile(np.arange(n_poe), int(all_indices / n_poe))

        poe_series = nested_array.reshape(all_indices)  # get the actual poe_values

        # additional series for the data held outside the nested array
        vs30_series = np.full(all_indices, vs30)
        calculation_id_idx = np.full(all_indices, 0)
        compatible_calc_idx = np.full(all_indices, 0)
        producer_config_idx = np.full(all_indices, 0)

        nloc_001_idx = np.full(all_indices, nloc_001)
        nloc_0_idx = np.full(all_indices, nloc_0)

        if True:
            print("nloc_001_idx.shape", nloc_001_idx.shape)
            print("nloc_0_idx.shape", nloc_001_idx.shape)
            log.debug(f"trt.shape {trt_idx.shape}")
            log.debug(f"trt {trt_idx}")
            log.debug(f"mag.shape {mag_idx.shape}")
            log.debug(f"mag {mag_idx}")
            log.debug(f"dist.shape {dist_idx.shape}")
            log.debug(f"dist {dist_idx}")
            log.debug(f"eps.shape {eps_idx.shape}")
            log.debug(f"eps {eps_idx}")
            log.debug(f"imt.shape {imt_idx.shape}")
            log.debug(f"imt {imt_idx}")
            log.debug(f"rlz.shape {rlz_idx.shape}")
            log.debug(f"rlz {rlz_idx}")
            log.debug(f"poe_series.shape {poe_series.shape}")
            log.debug(f"values {poe_series}")

        # Build the categorised series as pa.DictionaryArray objects
        # compatible_calc_cat = pa.DictionaryArray.from_arrays(compatible_calc_idx, [compatible_calc_fk])
        # producer_config_cat = pa.DictionaryArray.from_arrays(producer_config_idx, [producer_config_fk])
        # calculation_id_cat = pa.DictionaryArray.from_arrays(calculation_id_idx, [calculation_id])

        nloc_001_cat = pa.DictionaryArray.from_arrays(nloc_001_idx, ["MRO"])  # [l.code for l in nloc_001_locations])
        nloc_0_cat = pa.DictionaryArray.from_arrays(nloc_0_idx, ["MRO"])  # nloc_0_map.keys())

        # TODO make these more useful
        mag_bin_names = [str(x) for x in range(n_mag)]
        dist_bin_names = [str(x) for x in range(n_dist)]
        eps_bin_names = [str(x) for x in range(n_eps)]

        trt_cat = pa.DictionaryArray.from_arrays(trt_idx, trt_values)
        mag_cat = pa.DictionaryArray.from_arrays(mag_idx, mag_bin_names)
        dist_cat = pa.DictionaryArray.from_arrays(dist_idx, dist_bin_names)
        eps_cat = pa.DictionaryArray.from_arrays(eps_idx, eps_bin_names)

        imt_cat = pa.DictionaryArray.from_arrays(imt_idx, list(disagg_rlzs.imt))
        rlz_cat = pa.DictionaryArray.from_arrays(rlz_idx, list(disagg_rlzs.extra))
        # print(trt_cat)
        # print(imt_cat)
        # print(rlz_cat)

        # sources_digest_cat = pa.DictionaryArray.from_arrays(rlz_idx, sources_digests)
        # gmms_digest_cat = pa.DictionaryArray.from_arrays(rlz_idx, gmms_digests)

        yield pa.RecordBatch.from_arrays(
            [
                # compatible_calc_cat,
                # producer_config_cat,
                # calculation_id_cat,
                nloc_001_cat,
                nloc_0_cat,
                trt_cat,
                mag_cat,
                dist_cat,
                eps_cat,
                imt_cat,
                rlz_cat,
                vs30_series,
                poe_series,
                # sources_digest_cat,
                # gmms_digest_cat,
                # values_series,
            ],
            [
                # "compatible_calc_fk", "producer_config_fk", "calculation_id",
                "nloc_001",
                "nloc_0",
                "trt",
                "mag",
                "dist",
                "eps",
                "imt",
                "rlz",
                "vs30",
                "poe",
                # " sources_digest", "gmms_digest", "values"
            ],
        )

    # create a schema...
    poe_type = pa.float64()  ## CHECK if this is enough res, or float32 float64
    vs30_type = pa.int32()
    dict_type = pa.dictionary(pa.int32(), pa.string(), True)
    schema = pa.schema(
        [
            # ("compatible_calc_fk", dict_type),
            # ("producer_config_fk", dict_type),
            # ("calculation_id", dict_type),
            ("nloc_001", dict_type),
            ("nloc_0", dict_type),
            ('trt', dict_type),
            ('mag', dict_type),
            ('dist', dict_type),
            ('eps', dict_type),
            ('imt', dict_type),
            ('rlz', dict_type),
            ('vs30', vs30_type),
            # ('sources_digest', dict_type),
            # ('gmms_digest', dict_type),
            ("poe", poe_type),
        ]
    )

    return pa.RecordBatchReader.from_batches(schema, build_batch(disagg_rlzs, nloc_0=0, nloc_001=0))


def extract_to_dataset(hdf5_file: pathlib.Path, dataset_folder):
    model_generator = disaggs_to_record_batch_reader(
        hdf5_file, calculation_id=hdf5_file.parent.name, compatible_calc_fk="A_A", producer_config_fk="A_B"
    )
    pyarrow_dataset.append_models_to_dataset(model_generator, str(OUTPUT_FOLDER))
    print(f"processed models in {hdf5_file.parent.name}")


def load_dataframe(dataset_folder):
    dataset = ds.dataset(dataset_folder, format='parquet', partitioning='hive')
    table = dataset.to_table()
    return table.to_pandas()


WORKING = pathlib.Path('/GNSDATA/LIB/toshi-hazard-store/WORKING/DISAGG')
OUTPUT_FOLDER = WORKING / "ARROW" / "DIRECT_DISAGG"

# hdf5_file = WORKING / 'openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazoxMzU5MTQ1' / 'calc_1.hdf5' # bad file 4
hdf5_file = WORKING / 'openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazo2OTI2MTg2' / 'calc_1.hdf5'  # bad file 3
csvfile = WORKING / 'openquake_csv_archive-T3BlbnF1YWtlSGF6YXJkVGFzazo2OTI2MTg2' / 'TRT_Mag_Dist_Eps-0_1.csv'  # last
import random

if __name__ == '__main__':

    """
    disagg = pathlib.Path('/GNSDATA/LIB/toshi-hazard-store/WORKING/DISAGG')
    bad_file_1 = disagg / 'calc_1.hdf5'
    bad_file_2 = disagg / 'openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazoxMDYzMzU3' / 'calc_1.hdf5'
    bad_file_3 = disagg / 'openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazo2OTI2MTg2' / 'calc_1.hdf5'
    bad_file_4 = disagg / 'openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazoxMzU5MTQ1' / 'calc_1.hdf5'
    """

    # extract_to_dataset(hdf5_file, dataset_folder=OUTPUT_FOLDER)

    df0 = load_dataframe(dataset_folder=OUTPUT_FOLDER)
    df1 = pd.read_csv(str(csvfile), header=1)

    def reshape_csv_dataframe(df1):
        rlz_cols = [cname for cname in df1.columns if 'rlz' in cname]

        def generate_subtables(df1, rlz_cols):
            for idx, key in enumerate(rlz_cols):
                drop_cols = rlz_cols.copy()
                drop_cols.remove(key)
                sub_df = df1.drop(columns=drop_cols)
                yield sub_df.rename(columns={key: "rlz"})

        return pd.concat(generate_subtables(df1, rlz_cols))

    def compare_hdf5_csv(df_hdf5, df_csv):
        print(f"HDF shape, {df_hdf5.shape}")
        print(f"HDF cols, {df_hdf5.columns}")
        print(f"HDF mag, {len(df_hdf5['mag'].unique())} {df_hdf5['mag'].unique()}")
        print(f"HDF eps, {len(df_hdf5['eps'].unique())} {df_hdf5['eps'].unique()}")
        print(f"HDF imt, {len(df_hdf5['imt'].unique())}")

        print()
        print(f"CSV shape, {df_csv.shape}")
        print(f"CSV cols, {df_csv.columns}")
        print(f"CSV mag, {len(df_csv['mag'].unique())} {df_csv['mag'].unique()}")
        print(f"CSV eps, {len(df_csv['eps'].unique())} {df_csv['mag'].unique()}")
        print(f"CSV imt, {len(df_csv['imt'].unique())}")

    # compare_hdf5_csv(df0, df1)

    print()
    print('RESHAPING')
    print('============================')
    df2 = reshape_csv_dataframe(df1)
    # compare_hdf5_csv(df0, df2)

    def random_spot_checks(df_hdf, df_csv):
        hdf_mag = df_hdf['mag'].unique().tolist()
        hdf_eps = df_hdf['eps'].unique().tolist()
        hdf_dist = df_hdf['dist'].unique().tolist()

        csv_mag = df_csv['mag'].unique().tolist()
        csv_eps = df_csv['eps'].unique().tolist()
        csv_dist = df_csv['dist'].unique().tolist()

        assert len(hdf_mag) == (len(csv_mag))
        assert len(hdf_eps) == (len(csv_eps))
        assert len(hdf_dist) == (len(csv_dist))

        eps_idx = random.randint(0, len(hdf_eps) - 1)
        mag_idx = random.randint(0, len(hdf_mag) - 1)
        dist_idx = random.randint(0, len(hdf_dist) - 1)

        flt_hdf = (
            (df_hdf.eps == hdf_eps[eps_idx]) & (df_hdf.mag == hdf_mag[mag_idx]) & (df_hdf.dist == hdf_dist[dist_idx])
        )
        flt_csv = (
            (df_csv.eps == csv_eps[eps_idx]) & (df_csv.mag == csv_mag[mag_idx]) & (df_csv.dist == csv_dist[dist_idx])
        )

        # print(flt)
        print(df_hdf[flt_hdf])
        print()
        print(df_csv[flt_csv])

    random_spot_checks(df0, df2)

    # print(df.head(225))


def reshape_csv_classic_dataframe(df1):
    collapse_cols = [cname for cname in df1.columns if 'poe' in cname]

    def generate_subtables(df1, collapse_cols):
        for idx, key in enumerate(collapse_cols):
            drop_cols = collapse_cols.copy()
            drop_cols.remove(key)
            sub_df = df1.drop(columns=drop_cols)
            yield sub_df.rename(columns={key: "poe"})

    return pd.concat(generate_subtables(df1, collapse_cols))
