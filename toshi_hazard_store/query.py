"""Queries for saving and retrieving openquake hazard results with convenience."""
from typing import Iterable, Iterator

import toshi_hazard_store.model as model


def batch_save_hcurve_stats(toshi_id: str, models: Iterable[model.ToshiOpenquakeHazardCurveStats]) -> None:
    """Save list of ToshiOpenquakeHazardCurveStats updating hash and range keys."""
    with model.ToshiOpenquakeHazardCurveStats.batch_write() as batch:
        for item in models:
            item.hazard_solution_id = toshi_id
            item.imt_loc_agg_rk = f"{item.imt_code}:{item.location_code}:{item.aggregation}"
            batch.save(item)


def batch_save_hcurve_rlzs(toshi_id, models: Iterable[model.ToshiOpenquakeHazardCurveRlzs]):
    """Save list of ToshiOpenquakeHazardCurveRlzs updating hash and range keys."""
    with model.ToshiOpenquakeHazardCurveRlzs.batch_write() as batch:
        for item in models:
            item.hazard_solution_id = toshi_id
            item.imt_loc_rlz_rk = f"{item.imt_code}:{item.location_code}:{item.rlz_id}"
            batch.save(item)


mOHCS = model.ToshiOpenquakeHazardCurveStats
mOHCR = model.ToshiOpenquakeHazardCurveRlzs
mOHM = model.ToshiOpenquakeHazardMeta


def get_hazard_stats_curves(
    hazard_solution_id: str,
    imt_codes: Iterable[str] = None,
    loc_codes: Iterable[str] = None,
    agg_codes: Iterable[str] = None,
) -> Iterator[mOHCS]:
    """Use ToshiOpenquakeHazardCurveStats.imt_loc_agg_rk range key as much as possible."""

    range_key_first_val = ""
    condition_expr = None

    if imt_codes:
        first_imt = sorted(imt_codes)[0]
        range_key_first_val += f"{first_imt}"
        condition_expr = condition_expr & mOHCS.imt_code.is_in(*imt_codes)
    if loc_codes:
        condition_expr = condition_expr & mOHCS.location_code.is_in(*loc_codes)
    if agg_codes:
        condition_expr = condition_expr & mOHCS.aggregation.is_in(*agg_codes)

    if imt_codes and loc_codes:
        first_loc = sorted(loc_codes)[0]
        range_key_first_val += f":{first_loc}"
    if imt_codes and loc_codes and agg_codes:
        first_agg = sorted(agg_codes)[0]
        range_key_first_val += f":{first_agg}"

    for hit in model.ToshiOpenquakeHazardCurveStats.query(
        hazard_solution_id, mOHCS.imt_loc_agg_rk >= range_key_first_val, filter_condition=condition_expr
    ):
        yield (hit)


def get_hazard_rlz_curves(
    hazard_solution_id: str,
    imt_codes: Iterable[str] = None,
    loc_codes: Iterable[str] = None,
    rlz_ids: Iterable[str] = None,
) -> Iterator[mOHCR]:
    """Use ToshiOpenquakeHazardCurveRlzs.imt_loc_agg_rk range key as much as possible."""

    range_key_first_val = ""
    condition_expr = None

    if imt_codes:
        first_imt = sorted(imt_codes)[0]
        range_key_first_val += f"{first_imt}"
        condition_expr = condition_expr & mOHCR.imt_code.is_in(*imt_codes)
    if loc_codes:
        condition_expr = condition_expr & mOHCR.location_code.is_in(*loc_codes)
    if rlz_ids:
        condition_expr = condition_expr & mOHCR.rlz_id.is_in(*rlz_ids)

    if imt_codes and loc_codes:
        first_loc = sorted(loc_codes)[0]
        range_key_first_val += f":{first_loc}"
    if imt_codes and loc_codes and rlz_ids:
        first_rlz = sorted(rlz_ids)[0]
        range_key_first_val += f":{first_rlz}"

    print(f"range_key_first_val: {range_key_first_val}")
    print(condition_expr)

    for hit in model.ToshiOpenquakeHazardCurveRlzs.query(
        hazard_solution_id, mOHCR.imt_loc_rlz_rk >= range_key_first_val, filter_condition=condition_expr
    ):
        yield (hit)


def get_hazard_metadata(
    hazard_solution_ids: Iterable[str] = None,
    vs30_vals: Iterable[int] = None,
) -> Iterator[mOHM]:
    """Fetch ToshiOpenquakeHazardMeta based on criteria."""

    condition_expr = None
    if hazard_solution_ids:
        condition_expr = condition_expr & mOHM.hazard_solution_id.is_in(*hazard_solution_ids)
    if vs30_vals:
        condition_expr = condition_expr & mOHM.vs30.is_in(*vs30_vals)

    for hit in model.ToshiOpenquakeHazardMeta.query(
        "ToshiOpenquakeHazardMeta", filter_condition=condition_expr  # NB the partition key is the table name!
    ):
        yield (hit)
