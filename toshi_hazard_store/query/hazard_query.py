"""Queries for saving and retrieving openquake hazard results with convenience."""
import decimal
import itertools
import logging
from typing import Iterable, Iterator, Union

from nzshm_common.location.code_location import CodedLocation

import toshi_hazard_store.model as model

log = logging.getLogger(__name__)
# log.setLevel(logging.DEBUG)

mOQM = model.ToshiOpenquakeMeta
mRLZ = model.OpenquakeRealization
mHAG = model.HazardAggregation


def get_hazard_metadata_v3(
    haz_sol_ids: Iterable[str],
    vs30_vals: Iterable[int],
) -> Iterator[mOQM]:
    """Fetch ToshiOpenquakeHazardMeta based on criteria."""

    total_hits = 0
    for (tid, vs30) in itertools.product(haz_sol_ids, vs30_vals):
        sort_key_val = f"{tid}:{vs30}"
        log.debug('sort_key_val: %s' % sort_key_val)

        for hit in mOQM.query(
            "ToshiOpenquakeMeta",  # NB the partition key is the table name!
            range_key_condition=(mOQM.hazsol_vs30_rk == sort_key_val),
        ):
            total_hits += 1
            yield (hit)

    log.info('Total %s hits' % total_hits)


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


def get_rlz_curves_v3(
    locs: Iterable[str],
    vs30s: Iterable[int],
    rlzs: Iterable[int],
    tids: Iterable[str],
    imts: Iterable[str],
) -> Iterator[mRLZ]:
    """Query THS_OpenquakeRealization Table.

    :param locs: coded location codes e.g. ['-46.430~168.360']
    :param vs30s: vs30 values eg [400, 500]
    :param rlzs: realizations eg [0,1,2,3]
    :param tids:  toshi hazard_solution_ids e.. ['XXZ']
    :param imts: imt (IntensityMeasureType) values e.g ['PGA', 'SA(0.5)']

    :yield: model objects
    """

    def build_condition_expr(loc, vs30, rlz, tid):
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

        return expr & (mRLZ.vs30 == vs30) & (mRLZ.rlz == rlz) & (mRLZ.hazard_solution_id == tid)

    total_hits = 0
    for hash_location_code in get_hashes(locs):
        partition_hits = 0
        log.info('hash_key %s' % hash_location_code)
        hash_locs = list(filter(lambda loc: downsample_code(loc, 0.1) == hash_location_code, locs))

        for (hloc, tid, vs30, rlz) in itertools.product(hash_locs, tids, vs30s, rlzs):

            sort_key_first_val = f"{hloc}:{vs30}:{str(rlz).zfill(6)}:{tid}"
            condition_expr = build_condition_expr(hloc, vs30, rlz, tid)

            log.debug('sort_key_first_val: %s' % sort_key_first_val)
            log.debug('condition_expr: %s' % condition_expr)

            results = mRLZ.query(
                hash_location_code, mRLZ.sort_key == sort_key_first_val, filter_condition=condition_expr
            )

            # print(f"get_hazard_rlz_curves_v3: qry {qry}")
            log.debug("get_hazard_rlz_curves_v3: results %s" % results)
            for hit in results:
                partition_hits += 1
                hit.values = list(filter(lambda x: x.imt in imts, hit.values))
                yield (hit)

        total_hits += partition_hits
        log.info('hash_key %s has %s hits' % (hash_location_code, partition_hits))

    log.info('Total %s hits' % total_hits)


def get_hazard_curves(
    locs: Iterable[str],
    vs30s: Iterable[int],
    hazard_model_ids: Iterable[str],
    imts: Iterable[str],
    aggs: Union[Iterable[str], None] = None,
    local_cache: bool = False,
) -> Iterator[mHAG]:
    """Query HazardAggregation Table.

    :param locs: coded location codes e.g. ['-46.430~168.360']
    :param vs30s: vs30 values eg [400, 500]
    :param hazard_model_ids:  hazard model ids e.. ['NSHM_V1.0.4']
    :param imts: imt (IntensityMeasureType) values e.g ['PGA', 'SA(0.5)']
    :param aggs: aggregation values e.g. ['mean']

    :yield: model objects
    """
    aggs = aggs or ["mean", "0.1"]

    log.info("get_hazard_curves( %s" % locs)

    def build_condition_expr(loc, vs30, hid, agg):
        """Build the filter condition expression."""
        grid_res = decimal.Decimal(str(loc.split('~')[0]))
        places = grid_res.as_tuple().exponent

        res = float(decimal.Decimal(10) ** places)
        loc = downsample_code(loc, res)

        expr = None

        if places == -1:
            expr = mHAG.nloc_1 == loc
        elif places == -2:
            expr = mHAG.nloc_01 == loc
        elif places == -3:
            expr = mHAG.nloc_001 == loc
        else:
            assert 0

        return expr & (mHAG.vs30 == vs30) & (mHAG.imt == imt) & (mHAG.agg == agg) & (mHAG.hazard_model_id == hid)

    # TODO: use https://pypi.org/project/InPynamoDB/
    total_hits = 0
    for hash_location_code in get_hashes(locs):
        partition_hits = 0
        log.info('hash_key %s' % hash_location_code)
        hash_locs = list(filter(lambda loc: downsample_code(loc, 0.1) == hash_location_code, locs))

        for (hloc, hid, vs30, imt, agg) in itertools.product(hash_locs, hazard_model_ids, vs30s, imts, aggs):

            sort_key_first_val = f"{hloc}:{vs30}:{imt}:{agg}:{hid}"
            condition_expr = build_condition_expr(hloc, vs30, hid, agg)

            log.debug('sort_key_first_val: %s' % sort_key_first_val)
            log.debug('condition_expr: %s' % condition_expr)

            results = mHAG.query(
                hash_key=hash_location_code,
                range_key_condition=mHAG.sort_key == sort_key_first_val,
                filter_condition=condition_expr,
                # limit=10,
                # rate_limit=None,
                # last_evaluated_key=None
            )

            log.debug("get_hazard_rlz_curves_v3: results %s" % results)
            for hit in results:
                partition_hits += 1
                yield (hit)

        total_hits += partition_hits
        log.info('hash_key %s has %s hits' % (hash_location_code, partition_hits))

    log.info('Total %s hits' % total_hits)
