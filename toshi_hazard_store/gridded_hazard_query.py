"""Queries for saving and retrieving gridded hazard convenience."""

import logging
from typing import Iterable, Iterator

from toshi_hazard_store.model.gridded_hazard import GriddedHazard

log = logging.getLogger(__name__)

mGH = GriddedHazard


def get_one_gridded_hazard(
    hazard_model_id: str,
    location_grid_id: str,
    vs30: float,
    imt: str,
    agg: str,
    poe: float,
) -> Iterator[mGH]:
    """Fetch GriddedHazard based on single criteria."""

    qry = mGH.query(hazard_model_id, mGH.sort_key == f'{hazard_model_id}:{location_grid_id}:{vs30}:{imt}:{agg}:{poe}')
    log.debug(f"get_gridded_hazard: qry {qry}")
    for hit in qry:
        yield (hit)


def get_gridded_hazard(
    hazard_model_ids: Iterable[str],
    location_grid_ids: Iterable[str],
    vs30s: Iterable[float],
    imts: Iterable[str],
    aggs: Iterable[str],
    poes: Iterable[float],
) -> Iterator[mGH]:
    """Fetch GriddedHazard based on criteria."""

    # partition_key = f"{obj.hazard_model_id}"
    # sort_key = f"{obj.hazard_model_id}:{obj.location_grid_id}:{obj.vs30}:{obj.imt}:{obj.agg}:{obj.poe}"

    def build_sort_key(hazard_model_id, grid_ids, vs30s, imts, aggs, poes):
        """Build sort_key."""

        sort_key = hazard_model_id
        sort_key = sort_key + f":{sorted(grid_ids)[0]}" if grid_ids else sort_key
        sort_key = sort_key + f":{sorted(vs30s)[0]}" if grid_ids and vs30s else sort_key
        sort_key = sort_key + f":{sorted(imts)[0]}" if grid_ids and vs30s and imts else sort_key
        sort_key = sort_key + f":{sorted(aggs)[0]}" if grid_ids and vs30s and imts and aggs else sort_key
        sort_key = sort_key + f":{sorted(poes)[0]}" if grid_ids and vs30s and imts and aggs and poes else sort_key
        return sort_key

    def build_condition_expr(hazard_model_id, location_grid_ids, vs30s, imts, aggs, poes):
        """Build filter condition."""
        condition_expr = mGH.hazard_model_id == hazard_model_id
        if location_grid_ids:
            condition_expr = condition_expr & mGH.location_grid_id.is_in(*location_grid_ids)
        if vs30s:
            condition_expr = condition_expr & mGH.vs30.is_in(*vs30s)
        if imts:
            condition_expr = condition_expr & mGH.imt.is_in(*imts)
        if aggs:
            condition_expr = condition_expr & mGH.agg.is_in(*aggs)
        if poes:
            condition_expr = condition_expr & mGH.poe.is_in(*poes)
        return condition_expr

    # TODO: this can be parallelised/optimised.
    for hazard_model_id in hazard_model_ids:

        sort_key_first_val = build_sort_key(hazard_model_id, location_grid_ids, vs30s, imts, aggs, poes)
        condition_expr = build_condition_expr(hazard_model_id, location_grid_ids, vs30s, imts, aggs, poes)

        log.debug(f'sort_key_first_val {sort_key_first_val}')
        log.debug(f'condition_expr {condition_expr}')

        if sort_key_first_val:
            qry = mGH.query(hazard_model_id, mGH.sort_key >= sort_key_first_val, filter_condition=condition_expr)
        else:
            qry = mGH.query(
                hazard_model_id,
                mGH.sort_key >= " ",  # lowest printable char in ascii table is SPACE. (NULL is first control)
                filter_condition=condition_expr,
            )

        log.debug(f"get_gridded_hazard: qry {qry}")
        for hit in qry:
            yield (hit)
