"""Queries for saving and retrieving gridded hazard convenience."""

import itertools
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

    def build_condition_expr(hazard_model_id, location_grid_id, vs30, imt, agg, poe):
        """Build filter condition."""
        condition_expr = (
            (mGH.hazard_model_id == hazard_model_id)
            & (mGH.location_grid_id == location_grid_id)
            & (mGH.vs30 == vs30)
            & (mGH.imt == imt)
            & (mGH.agg == agg)
            & (mGH.poe == poe)
        )
        return condition_expr

    total_hits = 0
    for (hazard_model_id, grid_id, vs30, imt, agg, poe) in itertools.product(
        hazard_model_ids, location_grid_ids, vs30s, imts, aggs, poes
    ):

        sort_key_first_val = f"{hazard_model_id}:{grid_id}:{vs30}:{imt}:{agg}:{poe}"
        condition_expr = build_condition_expr(hazard_model_id, grid_id, vs30, imt, agg, poe)

        log.debug(f'sort_key_first_val {sort_key_first_val}')
        log.debug(f'condition_expr {condition_expr}')

        qry = mGH.query(hazard_model_id, mGH.sort_key == sort_key_first_val, filter_condition=condition_expr)

        log.debug(f"get_gridded_hazard: qry {qry}")
        for hit in qry:
            total_hits += 1
            yield (hit)

    log.info('Total %s hits' % total_hits)
