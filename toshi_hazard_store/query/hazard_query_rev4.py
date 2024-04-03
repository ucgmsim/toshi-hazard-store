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

from nzshm_common.location.code_location import CodedLocation

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

def block_query():
    locs = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in list(LOCATIONS_BY_ID.values())[:1]]

    mRLZ_V4 = hazard_models.HazardRealizationCurve

    mRLZ_V3 = toshi_hazard_store.model.openquake_models.OpenquakeRealization

    t3 = time.perf_counter()
    # print(f'got {count} hits')
    # print(f"rev 4 query  {t3 - t2:.6f} seconds")
    # print()
    print()
    print("V3 ....")
    count = 0

    # assert len(location.LOCATION_LISTS["NZ"]["locations"]) == 36
    # assert len(location.LOCATION_LISTS["SRWG214"]["locations"]) == 214
    # assert len(location.LOCATION_LISTS["ALL"]["locations"]) == 214 + 36 + 19480
    # assert len(location.LOCATION_LISTS["HB"]["locations"]) == 19480

    grid = load_grid('NZ_0_1_NB_1_1')

    for loc in [CodedLocation(o[0], o[1], 0.1) for o in grid]:
        rlz = None
        # nloc_001 = loc.resample(0.001).code
        for rlz in mRLZ_V3.query(
            loc.code,
            mRLZ_V3.sort_key >= "",
            filter_condition=(mRLZ_V3.hazard_solution_id == "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3")
            & (mRLZ_V3.rlz == 0),
        ):
            # print(rlz.nloc_001, rlz.nloc_01)
            count += 1
        if rlz:
            print(rlz.partition_key, rlz.sort_key, count)
            rlz = None
        else:
            print(loc.code, 'no hits')

    t4 = time.perf_counter()
    print(f'got {count} hits')
    print(f"rev 3 query  {t4- t3:.6f} seconds")

    print()
    assert 0

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
    for rlz in get_rlz_curves([loc.code for loc in locs], [275], ['PGA', 'SA(1.0)']):
        srcs = [registry.source_registry.get_by_hash(s).extra for s in rlz.source_digests]
        gmms = [registry.gmm_registry.get_by_hash(g).identity for g in rlz.gmm_digests]
        # print([rlz.nloc_001, rlz.vs30, rlz.imt, srcs, gmms, rlz.compatible_calc_fk, rlz.values[:4]])  # srcs, gmms,
        print(rlz.partition_key, rlz.sort_key, rlz.nloc_001, rlz.nloc_01, rlz.nloc_1, rlz.vs30)
        count += 1
        if count == 10:
            assert 0
    print(rlz)

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
        tids=["T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3", "T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI3"],
        imts=['PGA', 'SA(1.0)'],
    ):
        # print(r)
        print(rlz.partition_key, rlz.sort_key, rlz.nloc_001, rlz.nloc_01, rlz.nloc_1, rlz.vs30)
        count += 1

    print(rlz)
    t4 = time.perf_counter()
    print(f'got {count} hits')
    print(f"rev 3 query  {t4- t3:.6f} seconds")


if __name__ == '__main__':

    from toshi_hazard_store.query import hazard_query
    from toshi_hazard_store.model import OpenquakeRealization
    import toshi_hazard_store.model

    from nzshm_common.grids import load_grid
    from nzshm_common import location

    t0 = time.perf_counter()
    from nzshm_model import branch_registry

    t1 = time.perf_counter()

    logging.basicConfig(level=logging.ERROR)
    log.info(f"nzshm-model import took {t1 - t0:.6f} seconds")

    from nzshm_common.location.location import LOCATIONS_BY_ID

    block_query()

    # demo_query()
