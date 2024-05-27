"""Helpers for querying Hazard Realizations and related models  - Revision 4.

Provides efficient queries for the models: **HazardRealizationCurve*.*

Functions:

 - **get_rlz_curves)**   - returns iterator of matching OpenquakeRealization objects.

"""

import decimal
import itertools
import logging
import time
from typing import Iterable, Iterator

from nzshm_common.location.coded_location import CodedLocation

from toshi_hazard_store.model.revision_4 import hazard_models

log = logging.getLogger(__name__)


def downsample_code(loc_code, res):
    lt = loc_code.split('~')
    assert len(lt) == 2
    return CodedLocation(lat=float(lt[0]), lon=float(lt[1]), resolution=res).code


def get_hashes(locs: Iterable[str]):
    hashes = set()
    for loc in locs:
        lt = loc.split('~')
        assert len(lt) == 2
        hashes.add(downsample_code(loc, 0.1))
    return sorted(list(hashes))


def get_rlz_curves(
    locs: Iterable[str],
    vs30s: Iterable[int],
    imts: Iterable[str],
) -> Iterator[hazard_models.HazardRealizationCurve]:
    """Query the HazardRealizationCurve table.

    Parameters:
        locs: coded location codes e.g. ['-46.430~168.360']
        vs30s: vs30 values eg [400, 500]
        imts: imt (IntensityMeasureType) values e.g ['PGA', 'SA(0.5)']

    Yields:
        HazardRealizationCurve models
    """

    # table classes may be rebased (for testing), this makes sure we always get the current class definition.
    mRLZ = hazard_models.__dict__['HazardRealizationCurve']

    def build_condition_expr(loc, vs30, imt):
        """Build the filter condition expression."""
        grid_res = decimal.Decimal(str(loc.split('~')[0]))
        places = grid_res.as_tuple().exponent

        res = float(decimal.Decimal(10) ** places)
        loc = downsample_code(loc, res)
        expr = None

        if places == -1:
            expr = mRLZ.nloc_1 == loc
        elif places == -2:
            expr = mRLZ.nloc_01 == loc
        elif places == -3:
            expr = mRLZ.nloc_001 == loc
        else:
            assert 0
        return expr & (mRLZ.vs30 == vs30) & (mRLZ.imt == imt)

    total_hits = 0
    for hash_location_code in get_hashes(locs):
        partition_hits = 0
        log.debug('hash_key %s' % hash_location_code)
        hash_locs = list(filter(lambda loc: downsample_code(loc, 0.1) == hash_location_code, locs))

        for hloc, vs30, imt in itertools.product(hash_locs, vs30s, imts):

            sort_key_first_val = f"{hloc}:{str(vs30).zfill(4)}:{imt}"
            condition_expr = build_condition_expr(hloc, vs30, imt)

            log.debug('sort_key_first_val: %s' % sort_key_first_val)
            log.debug('condition_expr: %s' % condition_expr)

            results = mRLZ.query(
                hash_location_code,
                mRLZ.sort_key >= sort_key_first_val,
                filter_condition=condition_expr,
            )

            # print(f"get_hazard_rlz_curves_v3: qry {qry}")
            log.debug("get_hazard_rlz_curves_v3: results %s" % results)
            for hit in results:
                partition_hits += 1
                # hit.values = list(filter(lambda x: x.imt in imts, hit.values))
                yield (hit)

        total_hits += partition_hits
        log.debug('hash_key %s has %s hits' % (hash_location_code, partition_hits))

    log.info('Total %s hits' % total_hits)


##
# DEMO code below, to migrate to tests and/or docs
##
# flake8: noqa


def block_query():

    import pandas

    from toshi_hazard_store.oq_import.oq_manipulate_hdf5 import migrate_nshm_uncertainty_string
    from toshi_hazard_store.oq_import.parse_oq_realizations import rlz_mapper_from_dataframes

    locs = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in list(LOCATIONS_BY_ID.values())[:1]]

    mMeta = toshi_hazard_store.model.openquake_models.ToshiOpenquakeMeta
    mRLZ_V4 = hazard_models.HazardRealizationCurve

    mRLZ_V3 = toshi_hazard_store.model.openquake_models.OpenquakeRealization

    hazard_solution_id = "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3"
    query = mMeta.query("ToshiOpenquakeMeta", mMeta.hazsol_vs30_rk == f"{hazard_solution_id}:275")

    meta = next(query)
    gsim_lt = pandas.read_json(meta.gsim_lt)
    source_lt = pandas.read_json(meta.src_lt)
    rlz_lt = pandas.read_json(meta.rlz_lt)

    # apply the gsim migrations
    gsim_lt["uncertainty"] = gsim_lt["uncertainty"].map(migrate_nshm_uncertainty_string)

    rlz_map = rlz_mapper_from_dataframes(source_lt=source_lt, gsim_lt=gsim_lt, rlz_lt=rlz_lt)

    # print(rlz_map)

    t3 = time.perf_counter()

    print()
    print("V3 ....")

    # assert len(location.LOCATION_LISTS["NZ"]["locations"]) == 36
    # assert len(location.LOCATION_LISTS["SRWG214"]["locations"]) == 214
    # assert len(location.LOCATION_LISTS["ALL"]["locations"]) == 214 + 36 + 19480
    # assert len(location.LOCATION_LISTS["HB"]["locations"]) == 19480

    t2 = time.perf_counter()
    count = 0
    for rlz in mRLZ_V4.query(
        '-42.4~171.2',
        mRLZ_V4.sort_key >= '',
        filter_condition=(mRLZ_V4.imt == "PGA"),  # & (mRLZ_V4.nloc_1 == '-37.0~175.0')
    ):
        print(rlz.partition_key, rlz.sort_key, rlz.nloc_001, rlz.nloc_01, rlz.nloc_1, rlz.vs30)
        count += 1
    # print(res)

    t3 = time.perf_counter()
    print(f'got {count} hits')
    print(f"rev 4 query  {t3 - t2:.6f} seconds")


def demo_query():
    registry = branch_registry.Registry()

    locs = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in list(LOCATIONS_BY_ID.values())[5:6]]

    t2 = time.perf_counter()
    count = 0
    rlz = None
    for rlz in get_rlz_curves([loc.code for loc in locs], [275], ['PGA', 'SA(1.0)']):
        srcs = [registry.source_registry.get_by_hash(s).extra for s in rlz.source_digests]
        gmms = [registry.gmm_registry.get_by_hash(g).identity for g in rlz.gmm_digests]
        # print([rlz.nloc_001, rlz.vs30, rlz.imt, srcs, gmms, rlz.compatible_calc_fk, rlz.values[:4]])  # srcs, gmms,
        # print(rlz.partition_key, rlz.sort_key, rlz.nloc_001, rlz.nloc_01, rlz.nloc_1, rlz.vs30)
        count += 1
        # if count == 10:
        #     assert 0
    print(rlz) if rlz else print("V4 no hits")

    t3 = time.perf_counter()
    print(f'got {count} hits')
    print(f"rev 4 query  {t3 - t2:.6f} seconds")
    print()
    print()
    print("V3 ....")
    count = 0
    for rlz in hazard_query.get_rlz_curves_v3(
        locs=[loc.code for loc in locs],
        vs30s=[275],
        rlzs=[x for x in range(21)],
        tids=["T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNA=="],
        imts=['PGA', 'SA(1.0)'],
    ):
        # print(r)
        # print(rlz.partition_key, rlz.sort_key, rlz.nloc_001, rlz.nloc_01, rlz.nloc_1, rlz.vs30)
        count += 1

    print(rlz)
    t4 = time.perf_counter()
    print(f'got {count} hits')
    print(f"rev 3 query  {t4- t3:.6f} seconds")


def test_query():

    test_loc = "-42.450~171.210"

    wd = pathlib.Path(__file__).parent
    gtfile = wd / "GT_HAZ_IDs_R2VuZXJhbFRhc2s6MTMyODQxNA==.json"
    print(gtfile)
    assert gtfile.exists()
    gt_info = json.load(open(str(gtfile)))

    tids = [edge['node']['child']['hazard_solution']["id"] for edge in gt_info['data']['node']['children']['edges']]
    # print(tids)

    t3 = time.perf_counter()
    print("V3 ....")
    count = 0
    for rlz in hazard_query.get_rlz_curves_v3(
        locs=[test_loc],
        vs30s=[275],
        rlzs=[x for x in range(21)],
        tids=tids,  # ["T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNA=="],
        imts=['PGA'],
    ):
        # print(r)
        # print(rlz.partition_key, rlz.sort_key, rlz.nloc_001, rlz.nloc_01, rlz.nloc_1, rlz.vs30)
        count += 1

    print(rlz) if rlz else print("V3 no hits")
    t4 = time.perf_counter()
    print(f'got {count} hits')
    print(f"rev 3 query  {t4- t3:.6f} seconds")


if __name__ == '__main__':

    import json
    import pathlib

    from nzshm_common import location
    from nzshm_common.grids import load_grid

    import toshi_hazard_store.model
    from toshi_hazard_store.model import OpenquakeRealization
    from toshi_hazard_store.query import hazard_query

    t0 = time.perf_counter()
    from nzshm_model import branch_registry

    t1 = time.perf_counter()

    logging.basicConfig(level=logging.ERROR)
    log.info(f"nzshm-model import took {t1 - t0:.6f} seconds")

    from nzshm_common.location.location import LOCATIONS_BY_ID

    # block_query()
    # demo_query()
    test_query()
