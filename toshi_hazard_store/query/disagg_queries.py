"""Queries for saving and retrieving gridded hazard convenience."""

import decimal
import logging
from typing import Iterable, Iterator, List, Type, Union

from nzshm_common.location.code_location import CodedLocation

from toshi_hazard_store.model import (
    AggregationEnum,
    DisaggAggregationExceedance,
    DisaggAggregationOccurence,
    ProbabilityEnum,
)

from .hazard_query import downsample_code, get_hashes, have_mixed_length_vs30s

# from pynamodb.expressions.condition import Condition


log = logging.getLogger(__name__)

# aliases for models
mDAE = DisaggAggregationExceedance
mDAO = DisaggAggregationOccurence


def get_one_disagg_aggregation(
    hazard_model_id: str,
    hazard_agg: AggregationEnum,
    disagg_agg: AggregationEnum,
    location: CodedLocation,
    vs30: float,
    imt: str,
    poe: ProbabilityEnum,
    model: Type[Union[mDAE, mDAO]] = mDAE,
) -> Union[mDAE, mDAO, None]:
    """Fetch model based on single model arguments."""

    qry = model.query(
        downsample_code(location, 0.1),
        range_key_condition=model.sort_key == f'{hazard_model_id}:{hazard_agg.value}:{disagg_agg.value}:'
        f'{location}:{vs30}:{imt}:{poe.name}',  # type: ignore
    )

    log.debug(f"get_one_disagg_aggregation: qry {qry}")
    result: List[Union[mDAE, mDAO]] = list(qry)
    assert len(result) in [0, 1]
    if len(result):
        return result[0]
    return None


def get_disagg_aggregates(
    hazard_model_ids: Iterable[str],
    disagg_aggs: Iterable[AggregationEnum],
    hazard_aggs: Iterable[AggregationEnum],
    locs: Iterable[CodedLocation],  # nloc_001
    vs30s: Iterable[int],
    imts: Iterable[str],
    poes: Iterable[ProbabilityEnum],
    model: Type[Union[mDAE, mDAO]] = mDAE,
) -> Iterator[Union[mDAE, mDAO]]:

    hazard_agg_keys = [a.value for a in hazard_aggs]
    disagg_agg_keys = [a.value for a in disagg_aggs]
    poe_keys = [a for a in poes]

    # print(poe_keys[0])

    def build_sort_key(locs, vs30s, imts, hazard_agg_keys, disagg_agg_keys, poe_keys, tids):
        """Build sort_key."""

        sort_key = ""
        sort_key = sort_key + f"{sorted([_id for _id in tids])[0]}" if tids else sort_key
        sort_key = sort_key + f":{sorted(hazard_agg_keys)[0]}" if tids and hazard_agg_keys else sort_key
        sort_key = (
            sort_key + f":{sorted(disagg_agg_keys)[0]}" if tids and hazard_agg_keys and disagg_agg_keys else sort_key
        )
        sort_key = (
            sort_key + f":{sorted(locs)[0]}" if tids and hazard_agg_keys and disagg_agg_keys and locs else sort_key
        )
        sort_key = (
            sort_key + f":{sorted(vs30s)[0]}"
            if tids and hazard_agg_keys and disagg_agg_keys and locs and vs30s
            else sort_key
        )
        if have_mixed_length_vs30s(vs30s):  # we must stop the sort_key build here
            return sort_key

        sort_key = (
            sort_key + f":{sorted(imts)[0]}"
            if tids and hazard_agg_keys and disagg_agg_keys and locs and vs30s and imts
            else sort_key
        )
        sort_key = (
            sort_key + f":{sorted([p.name for p in poe_keys])[0]}"
            if tids and hazard_agg_keys and disagg_agg_keys and locs and vs30s and imts and poe_keys
            else sort_key
        )
        return sort_key

    def build_condition_expr(locs, vs30s, imts, hazard_agg_keys, disagg_agg_keys, poe_keys, hazard_model_ids):
        """Build filter condition."""
        ## TODO REFACTOR ME ... using the resolution of first loc is not ideal
        grid_resolution = decimal.Decimal(str(list(locs)[0].split('~')[0]))
        places = grid_resolution.as_tuple().exponent
        resolution = float(decimal.Decimal(10) ** places)
        locs = [downsample_code(loc, resolution) for loc in locs]

        condition_expr = None

        if places == -1:
            condition_expr = condition_expr & model.nloc_1.is_in(*locs)
        if places == -2:
            condition_expr = condition_expr & model.nloc_01.is_in(*locs)
        if places == -3:
            condition_expr = condition_expr & model.nloc_001.is_in(*locs)

        if vs30s:
            condition_expr = condition_expr & model.vs30.is_in(*vs30s)
        if imts:
            condition_expr = condition_expr & model.imt.is_in(*imts)
        if hazard_aggs:
            condition_expr = condition_expr & model.hazard_agg.is_in(*hazard_agg_keys)
        if disagg_aggs:
            condition_expr = condition_expr & model.disagg_agg.is_in(*disagg_agg_keys)
        if poe_keys:
            condition_expr = condition_expr & model.probability.is_in(*poe_keys)
        if hazard_model_ids:
            condition_expr = condition_expr & model.hazard_model_id.is_in(*hazard_model_ids)

        log.debug(f'query condition {condition_expr}')
        return condition_expr

    # TODO: this can be parallelised/optimised.
    for hash_location_code in get_hashes(locs):

        log.debug('hash_key %s' % hash_location_code)

        hash_locs = list(filter(lambda loc: downsample_code(loc, 0.1) == hash_location_code, locs))
        sort_key_first_val = build_sort_key(
            hash_locs, vs30s, imts, hazard_agg_keys, disagg_agg_keys, poe_keys, hazard_model_ids
        )
        condition_expr = build_condition_expr(
            hash_locs, vs30s, imts, hazard_agg_keys, disagg_agg_keys, poe_keys, hazard_model_ids
        )

        log.debug(f'model {model}')
        log.debug(f'hash_location_code {hash_location_code}')
        log.debug(f'sort_key_first_val {sort_key_first_val}')
        log.debug(f'condition_expr {condition_expr}')

        if sort_key_first_val:
            qry = model.query(
                hash_location_code,
                model.sort_key >= sort_key_first_val,  # type: ignore
                filter_condition=condition_expr,
            )
        else:
            qry = model.query(
                hash_location_code,
                model.sort_key >= " ",  # type: ignore # lowest printable char in ascii table is SPACE.
                filter_condition=condition_expr,
            )

        log.debug(f"get_disagg_aggregates: qry {qry}")

        for hit in qry:
            yield hit
