"""Queries for saving and retrieving openquake hazard results with convenience."""
from typing import Iterable, Iterator

import toshi_hazard_store.model as model


def batch_save_hcurve_stats(toshi_id: str, models: Iterable[model.ToshiOpenquakeHazardCurveStats]) -> None:
    """Save list of ToshiOpenquakeHazardCurveStats updating hash and range keys."""
    with model.ToshiOpenquakeHazardCurveStats.batch_write() as batch:
        for item in models:
            item.haz_sol_id = toshi_id
            item.imt_loc_agg_rk = f"{item.imt}:{item.loc}:{item.agg}"
            batch.save(item)


def batch_save_hcurve_rlzs(toshi_id: str, models: Iterable[model.ToshiOpenquakeHazardCurveRlzs]) -> None:
    """Save list of ToshiOpenquakeHazardCurveRlzs updating hash and range keys."""
    with model.ToshiOpenquakeHazardCurveRlzs.batch_write() as batch:
        for item in models:
            item.haz_sol_id = toshi_id
            item.imt_loc_rlz_rk = f"{item.imt}:{item.loc}:{item.rlz}"
            batch.save(item)


def batch_save_hcurve_rlzs_v2(toshi_id: str, models: Iterable[model.ToshiOpenquakeHazardCurveRlzsV2]) -> None:
    """Save list of ToshiOpenquakeHazardCurveRlzsV2 updating hash and range keys."""
    with model.ToshiOpenquakeHazardCurveRlzsV2.batch_write() as batch:
        for item in models:
            item.haz_sol_id = toshi_id
            item.loc_rlz_rk = f"{item.loc}:{item.rlz}"
            batch.save(item)


def batch_save_hcurve_stats_v2(toshi_id: str, models: Iterable[model.ToshiOpenquakeHazardCurveStatsV2]) -> None:
    """Save list of ToshiOpenquakeHazardCurveRlzsV2 updating hash and range keys."""
    with model.ToshiOpenquakeHazardCurveStatsV2.batch_write() as batch:
        for item in models:
            item.haz_sol_id = toshi_id
            item.loc_agg_rk = f"{item.loc}:{item.agg}"
            batch.save(item)


mOHCS = model.ToshiOpenquakeHazardCurveStats
mOHCR = model.ToshiOpenquakeHazardCurveRlzs
mOHM = model.ToshiOpenquakeHazardMeta

mOHCS2 = model.ToshiOpenquakeHazardCurveStatsV2
mOHCR2 = model.ToshiOpenquakeHazardCurveRlzsV2


def get_hazard_stats_curves(
    haz_sol_id: str,
    imts: Iterable[str] = None,
    locs: Iterable[str] = None,
    aggs: Iterable[str] = None,
) -> Iterator[mOHCS]:
    """Use ToshiOpenquakeHazardCurveStats.imt_loc_agg_rk range key as much as possible."""

    range_key_first_val = ""
    condition_expr = None

    if imts:
        first_imt = sorted(imts)[0]
        range_key_first_val += f"{first_imt}"
        condition_expr = condition_expr & mOHCS.imt.is_in(*imts)
    if locs:
        condition_expr = condition_expr & mOHCS.loc.is_in(*locs)
    if aggs:
        condition_expr = condition_expr & mOHCS.agg.is_in(*aggs)

    if imts and locs:
        first_loc = sorted(locs)[0]
        range_key_first_val += f":{first_loc}"
    if imts and locs and aggs:
        first_agg = sorted(aggs)[0]
        range_key_first_val += f":{first_agg}"

    print(f"range_key_first_val: {range_key_first_val}")
    print(condition_expr)

    if range_key_first_val:
        qry = mOHCS.query(haz_sol_id, mOHCS.imt_loc_agg_rk >= range_key_first_val, filter_condition=condition_expr)
    else:
        qry = mOHCS.query(
            haz_sol_id,
            mOHCS.imt_loc_agg_rk >= " ",  # lowest printable char in ascii table is SPACE. (NULL is first control)
            filter_condition=condition_expr,
        )

    print(f"get_hazard_stats_curves: qry {qry}")
    for hit in qry:
        yield (hit)


def get_hazard_rlz_curves(
    haz_sol_id: str,
    imts: Iterable[str] = None,
    locs: Iterable[str] = None,
    rlzs: Iterable[str] = None,
) -> Iterator[mOHCR]:
    """Use ToshiOpenquakeHazardCurveRlzs.imt_loc_agg_rk range key as much as possible."""

    range_key_first_val = ""
    condition_expr = None

    if imts:
        first_imt = sorted(imts)[0]
        range_key_first_val += f"{first_imt}"
        condition_expr = condition_expr & mOHCR.imt.is_in(*imts)
    if locs:
        condition_expr = condition_expr & mOHCR.loc.is_in(*locs)
    if rlzs:
        condition_expr = condition_expr & mOHCR.rlz.is_in(*rlzs)

    if imts and locs:
        first_loc = sorted(locs)[0]
        range_key_first_val += f":{first_loc}"
    if imts and locs and rlzs:
        first_rlz = sorted(rlzs)[0]
        range_key_first_val += f":{first_rlz}"

    print(f"range_key_first_val: {range_key_first_val}")
    print(condition_expr)

    if range_key_first_val:
        qry = mOHCR.query(haz_sol_id, mOHCR.imt_loc_rlz_rk >= range_key_first_val, filter_condition=condition_expr)
    else:
        qry = mOHCR.query(
            haz_sol_id,
            mOHCR.imt_loc_rlz_rk >= " ",  # lowest printable char in ascii table is SPACE. (NULL is first control)
            filter_condition=condition_expr,
        )

    print(f"get_hazard_rlz_curves: qry {qry}")
    for hit in qry:
        yield (hit)


def get_hazard_rlz_curves_v2(
    haz_sol_id: str,
    imts: Iterable[str] = [],
    locs: Iterable[str] = [],
    rlzs: Iterable[str] = [],
) -> Iterator[mOHCR2]:
    """Use mOHCR2.loc_agg_rk range key as much as possible."""

    range_key_first_val = ""
    condition_expr = None

    # if imts:
    #     first_imt = sorted(imts)[0]
    #     range_key_first_val += f"{first_imt}"
    #     condition_expr = condition_expr & mOHCR.imt.is_in(*imts)
    if locs:
        condition_expr = condition_expr & mOHCR2.loc.is_in(*locs)
    if rlzs:
        condition_expr = condition_expr & mOHCR2.rlz.is_in(*rlzs)

    if locs:
        first_loc = sorted(locs)[0]
        range_key_first_val += f"{first_loc}"
    if locs and rlzs:
        first_rlz = sorted(rlzs)[0]
        range_key_first_val += f":{first_rlz}"

    print(f"range_key_first_val: {range_key_first_val}")
    print(condition_expr)

    if range_key_first_val:
        qry = mOHCR2.query(haz_sol_id, mOHCR2.loc_rlz_rk >= range_key_first_val, filter_condition=condition_expr)
    else:
        qry = mOHCR2.query(
            haz_sol_id,
            mOHCR2.loc_rlz_rk >= " ",  # lowest printable char in ascii table is SPACE. (NULL is first control)
            filter_condition=condition_expr,
        )

    print(f"get_hazard_rlz_curves_v2: qry {qry}")
    for hit in qry:
        if imts:
            hit.values = list(filter(lambda x: x.imt in imts, hit.values))
        yield (hit)


def get_hazard_stats_curves_v2(
    haz_sol_id: str,
    imts: Iterable[str] = [],
    locs: Iterable[str] = [],
    aggs: Iterable[str] = [],
) -> Iterator[mOHCS2]:
    """Use mOHCS2.loc_agg_rk range key as much as possible."""

    range_key_first_val = ""
    condition_expr = None

    if locs:
        condition_expr = condition_expr & mOHCS2.loc.is_in(*locs)
    if aggs:
        condition_expr = condition_expr & mOHCS2.agg.is_in(*aggs)

    if locs:
        first_loc = sorted(locs)[0]
        range_key_first_val += f"{first_loc}"
    if locs and aggs:
        first_agg = sorted(aggs)[0]
        range_key_first_val += f":{first_agg}"

    print(f"range_key_first_val: {range_key_first_val}")
    print(condition_expr)

    if range_key_first_val:
        qry = mOHCS2.query(haz_sol_id, mOHCS2.loc_agg_rk >= range_key_first_val, filter_condition=condition_expr)
    else:
        qry = mOHCS2.query(
            haz_sol_id,
            mOHCS2.loc_agg_rk >= " ",  # lowest printable char in ascii table is SPACE. (NULL is first control)
            filter_condition=condition_expr,
        )

    print(f"get_hazard_stats_curves_v2: qry {qry}")
    for hit in qry:
        if imts:
            hit.values = list(filter(lambda x: x.imt in imts, hit.values))
        yield (hit)


def get_hazard_metadata(
    haz_sol_ids: Iterable[str] = None,
    vs30_vals: Iterable[int] = None,
) -> Iterator[mOHM]:
    """Fetch ToshiOpenquakeHazardMeta based on criteria."""

    condition_expr = None
    if haz_sol_ids:
        condition_expr = condition_expr & mOHM.haz_sol_id.is_in(*haz_sol_ids)
    if vs30_vals:
        condition_expr = condition_expr & mOHM.vs30.is_in(*vs30_vals)

    for hit in model.ToshiOpenquakeHazardMeta.query(
        "ToshiOpenquakeHazardMeta", filter_condition=condition_expr  # NB the partition key is the table name!
    ):
        yield (hit)
