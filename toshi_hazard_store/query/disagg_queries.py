"""Queries for retrieving hazard aggregation models.

Functions:
    - get_one_disagg_aggregation
    - get_disagg_aggregates

Attributes:
    mDAE: alias for the  DisaggAggregationExceedance model
    mDAO: alias for the DisaggAggregationOccurence model
"""

import decimal
import itertools
import logging
from typing import Iterable, Iterator, List, Type, Union

from nzshm_common.location.coded_location import CodedLocation
from pynamodb.expressions.condition import Condition

from toshi_hazard_store.model import (
    AggregationEnum,
    DisaggAggregationExceedance,
    DisaggAggregationOccurence,
    ProbabilityEnum,
)

from .hazard_query import downsample_code, get_hashes

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
    """Query the DisaggAggregation table(s) for a single item

    Parameters:
        hazard_model_id: id for the required Hazard model
        hazard_agg: aggregation value e.g. 'mean'
        disagg_agg: aggregation value e.g. '0.9'
        location: id e.g. '-46.430~168.360'
        vs30: vs30 value eg 400
        imt: imt (IntensityMeasureType) value e.g 'PGA', 'SA(0.5)'
        poe:
        model: model type

    Yields:
        model object (one or none)
    """
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
    probabilities: Iterable[ProbabilityEnum],
    dbmodel: Type[Union[mDAE, mDAO]] = mDAE,
) -> Iterator[Union[mDAE, mDAO]]:
    """Query the DisaggAggregation table

    Parameters:
        hazard_model_ids: ids for the required Hazard models
        hazard_aggs: aggregation values e.g. ['mean']
        disagg_aggs: aggregation values e.g.  ['mean', '0.9']
        locs: e.g. ['-46.430~168.360']
        vs30s: vs30 value eg [400, 750]
        imts: imt (IntensityMeasureType) value e.g []'PGA', 'SA(0.5)']
        probabilities:
        dbmodel: model type

    Yields:
        model objects
    """

    hazard_agg_keys = [a.value for a in hazard_aggs]
    disagg_agg_keys = [a.value for a in disagg_aggs]
    probability_keys = [a for a in probabilities]

    def build_condition_expr(dbmodel, hazard_model_id, location, hazard_agg, disagg_agg, vs30, imt, probability):
        """Build the filter condition expression.

        f"{hazard_model_id}:{hazard_agg_key}:{hazard_agg_key}:{hloc}:{vs30}:{imt}:{probability}"
        """
        grid_res = decimal.Decimal(str(location.split('~')[0]))
        places = grid_res.as_tuple().exponent
        res = float(decimal.Decimal(10) ** places)
        loc = downsample_code(location, res)
        expr = None

        if places == -1:
            expr = dbmodel.nloc_1 == loc
        elif places == -2:
            expr = dbmodel.nloc_01 == loc
        elif places == -3:
            expr = dbmodel.nloc_001 == loc
        else:
            assert 0

        return (
            expr
            & (dbmodel.hazard_model_id == hazard_model_id)
            & (dbmodel.hazard_agg == hazard_agg)
            & (dbmodel.disagg_agg == disagg_agg)
            & (dbmodel.vs30 == vs30)
            & (dbmodel.imt == imt)
            & (dbmodel.probability == probability)
        )

    total_hits = 0
    for hash_location_code in get_hashes(locs):
        partition_hits = 0
        log.info('hash_key %s' % hash_location_code)
        hash_locs = list(filter(lambda loc: downsample_code(loc, 0.1) == hash_location_code, locs))

        for hloc, hazard_model_id, hazard_agg, disagg_agg, vs30, imt, probability in itertools.product(
            hash_locs, hazard_model_ids, hazard_agg_keys, disagg_agg_keys, vs30s, imts, probability_keys
        ):

            sort_key_first_val = f"{hazard_model_id}:{hazard_agg}:{disagg_agg}:{hloc}:{vs30}:{imt}:{probability.name}"
            sort_key_condition: Condition = (
                (mDAE.sort_key == sort_key_first_val) if dbmodel == mDAE else (mDAO.sort_key == sort_key_first_val)
            )
            condition_expr = build_condition_expr(
                dbmodel, hazard_model_id, hloc, hazard_agg, disagg_agg, vs30, imt, probability
            )

            log.debug('sort_key_first_val: %s' % sort_key_first_val)
            log.debug('condition_expr: %s' % condition_expr)

            results = dbmodel.query(
                hash_key=hash_location_code,
                range_key_condition=sort_key_condition,
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
