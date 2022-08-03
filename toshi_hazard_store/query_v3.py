"""Queries for saving and retrieving openquake hazard results with convenience."""
import decimal
from typing import Iterable, Iterator

# from toshi_hazard_store.utils import CodedLocation
from nzshm_common.location.code_location import CodedLocation

import toshi_hazard_store.model as model

mOQM = model.ToshiOpenquakeMeta
mRLZ = model.OpenquakeRealization
mHAG = model.HazardAggregation


def get_hazard_metadata_v3(
    haz_sol_ids: Iterable[str] = None,
    vs30_vals: Iterable[int] = None,
) -> Iterator[mOQM]:
    """Fetch ToshiOpenquakeHazardMeta based on criteria."""

    condition_expr = None
    if haz_sol_ids:
        condition_expr = condition_expr & mOQM.hazard_solution_id.is_in(*haz_sol_ids)
    if vs30_vals:
        condition_expr = condition_expr & mOQM.vs30.is_in(*vs30_vals)

    for hit in mOQM.query(
        "ToshiOpenquakeMeta", filter_condition=condition_expr  # NB the partition key is the table name!
    ):
        yield (hit)


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
    locs: Iterable[str] = [],  # nloc_001
    vs30s: Iterable[int] = [],  # vs30s
    rlzs: Iterable[int] = [],  # rlzs
    tids: Iterable[str] = [],  # toshi hazard_solution_ids
    imts: Iterable[str] = [],
) -> Iterator[mRLZ]:
    """Use mRLZ.sort_key as much as possible.


    f'{nloc_001}:{vs30s}:{rlzs}:{self.hazard_solution_id}'
    """

    def build_condition_expr(locs, vs30s, rlzs, tids):
        """Build filter condition."""
        ## TODO REFACTOR ME ... using the res of first loc is not ideal
        grid_res = decimal.Decimal(str(list(locs)[0].split('~')[0]))
        places = grid_res.as_tuple().exponent

        # print()
        # print(f'places {places} loc[0] {locs[0]}')

        res = float(decimal.Decimal(10) ** places)
        locs = [downsample_code(loc, res) for loc in locs]

        # print()
        # print(f'res {res} locs {locs}')
        condition_expr = None

        if places == -1:
            condition_expr = condition_expr & mRLZ.nloc_1.is_in(*locs)
        if places == -2:
            condition_expr = condition_expr & mRLZ.nloc_01.is_in(*locs)
        if places == -3:
            condition_expr = condition_expr & mRLZ.nloc_001.is_in(*locs)

        if vs30s:
            condition_expr = condition_expr & mRLZ.vs30.is_in(*vs30s)
        if rlzs:
            condition_expr = condition_expr & mRLZ.rlz.is_in(*rlzs)
        if tids:
            condition_expr = condition_expr & mRLZ.hazard_solution_id.is_in(*tids)

        return condition_expr

    def build_sort_key(locs, vs30s, rlzs, tids):
        """Build sort_key."""
        sort_key_first_val = ""
        first_loc = sorted(locs)[0]  # these need to be formatted to match the sort key 0.001 ?
        sort_key_first_val += f"{first_loc}"

        if vs30s:
            first_vs30 = sorted(vs30s)[0]
            sort_key_first_val += f":{first_vs30}"
        if vs30s and rlzs:
            first_rlz = str(sorted(rlzs)[0]).zfill(6)
            sort_key_first_val += f":{first_rlz}"
        if vs30s and rlzs and tids:
            first_tid = sorted(tids)[0]
            sort_key_first_val += f":{first_tid}"
        return sort_key_first_val

    # print('hashes', get_hashes(locs))
    # TODO: use https://pypi.org/project/InPynamoDB/
    for hash_location_code in get_hashes(locs):

        # print(f'hash_key {hash_location_code}')

        hash_locs = list(filter(lambda loc: downsample_code(loc, 0.1) == hash_location_code, locs))

        sort_key_first_val = build_sort_key(hash_locs, vs30s, rlzs, tids)
        condition_expr = build_condition_expr(hash_locs, vs30s, rlzs, tids)

        # print(f'sort_key_first_val: {sort_key_first_val}')
        # print(f'condition_expr: {condition_expr}')

        # expected_sort_key = '-41.300~174.780:750:000000:A_CRU'
        # expected_hash_key = '-41.3~174.8'

        # print()
        # print(expected_hash_key, expected_sort_key)
        # # assert 0

        if sort_key_first_val:
            qry = mRLZ.query(hash_location_code, mRLZ.sort_key >= sort_key_first_val, filter_condition=condition_expr)
        else:
            qry = mRLZ.query(
                hash_location_code,
                mRLZ.sort_key >= " ",  # lowest printable char in ascii table is SPACE. (NULL is first control)
                filter_condition=condition_expr,
            )

        # print(f"get_hazard_rlz_curves_v3: qry {qry}")
        for hit in qry:
            if imts:
                hit.values = list(filter(lambda x: x.imt in imts, hit.values))
            yield (hit)


def get_hazard_curves(
    locs: Iterable[str] = [],  # nloc_001
    vs30s: Iterable[int] = [],  # vs30s
    hazard_model_ids: Iterable[str] = [],  # hazard_model_ids
    imts: Iterable[str] = [],
    aggs: Iterable[str] = [],
) -> Iterator[mHAG]:
    """Use mHAG.sort_key as much as possible.


    f'{nloc_001}:{vs30s}:{hazard_model_id}'
    """

    def build_condition_expr(locs, vs30s, hids):
        """Build filter condition."""
        ## TODO REFACTOR ME ... using the res of first loc is not ideal
        grid_res = decimal.Decimal(str(list(locs)[0].split('~')[0]))
        places = grid_res.as_tuple().exponent

        # print()
        # print(f'places {places} loc[0] {locs[0]}')

        res = float(decimal.Decimal(10) ** places)
        locs = [downsample_code(loc, res) for loc in locs]

        condition_expr = None

        if places == -1:
            condition_expr = condition_expr & mHAG.nloc_1.is_in(*locs)
        if places == -2:
            condition_expr = condition_expr & mHAG.nloc_01.is_in(*locs)
        if places == -3:
            condition_expr = condition_expr & mHAG.nloc_001.is_in(*locs)

        if vs30s:
            condition_expr = condition_expr & mHAG.vs30.is_in(*vs30s)
        if imts:
            condition_expr = condition_expr & mHAG.imt.is_in(*imts)
        if aggs:
            condition_expr = condition_expr & mHAG.agg.is_in(*aggs)
        if hids:
            condition_expr = condition_expr & mHAG.hazard_model_id.is_in(*hids)

        return condition_expr

    def build_sort_key(locs, vs30s, hids):
        """Build sort_key."""
        sort_key_first_val = ""
        first_loc = sorted(locs)[0]  # these need to be formatted to match the sort key 0.001 ?
        sort_key_first_val += f"{first_loc}"

        if vs30s:
            first_vs30 = sorted(vs30s)[0]
            sort_key_first_val += f":{first_vs30}"
        if vs30s and imts:
            first_imt = sorted(imts)[0]
            sort_key_first_val += f":{first_imt}"
        if vs30s and imts and aggs:
            first_agg = sorted(aggs)[0]
            sort_key_first_val += f":{first_agg}"
        if vs30s and imts and aggs and hids:
            first_hid = sorted(hids)[0]
            sort_key_first_val += f":{first_hid}"
        return sort_key_first_val

    # print('hashes', get_hashes(locs))
    # TODO: use https://pypi.org/project/InPynamoDB/
    for hash_location_code in get_hashes(locs):

        print(f'hash_key {hash_location_code}')

        hash_locs = list(filter(lambda loc: downsample_code(loc, 0.1) == hash_location_code, locs))

        sort_key_first_val = build_sort_key(hash_locs, vs30s, hazard_model_ids)
        condition_expr = build_condition_expr(hash_locs, vs30s, hazard_model_ids)

        print(f'sort_key_first_val {sort_key_first_val}')
        print(f'condition_expr {condition_expr}')

        if sort_key_first_val:
            qry = mHAG.query(hash_location_code, mHAG.sort_key >= sort_key_first_val, filter_condition=condition_expr)
        else:
            qry = mHAG.query(
                hash_location_code,
                mHAG.sort_key >= " ",  # lowest printable char in ascii table is SPACE. (NULL is first control)
                filter_condition=condition_expr,
            )

        print(f"get_hazard_rlz_curves_v3: qry {qry}")
        for hit in qry:
            yield (hit)
