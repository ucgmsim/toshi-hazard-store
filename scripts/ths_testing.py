"""Console script for testing or pre-poulating toshi_hazard_store local cache."""
# noqa
import logging
import sys

import click
import pandas as pd
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATIONS, location_by_id

from toshi_hazard_store import model, query, query_v3
from toshi_hazard_store.config import DEPLOYMENT_STAGE, LOCAL_CACHE_FOLDER, REGION

# from nzshm_common.grids import load_grid, RegionGrid


NZ_01_GRID = 'NZ_0_1_NB_1_1'

ALL_AGG_VALS = [e.value for e in model.AggregationEnum]
ALL_IMT_VALS = [e.value for e in model.IntensityMeasureTypeEnum]
ALL_VS30_VALS = [e.value for e in model.VS30Enum][1:]  # drop the 0 value!
ALL_CITY_LOCS = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in LOCATIONS]


class PyanamodbConsumedHandler(logging.Handler):
    """
    This custom log handler works with PynamoDB and DEBUG level logging.

    Use it to count the cost of AWS queries.
    """

    def __init__(self, level=0) -> None:
        super().__init__(level)
        self.consumed = 0

    def reset(self):
        self.consumed = 0

    def emit(self, record):
        if "pynamodb/connection/base.py" in record.pathname and record.msg == "%s %s consumed %s units":
            self.consumed += record.args[2]


log = logging.getLogger()

count_cost_handler = PyanamodbConsumedHandler(logging.DEBUG)
log.addHandler(count_cost_handler)

# logging.basicConfig(level=logging.)
logging.getLogger('pynamodb').setLevel(logging.DEBUG)
# logging.getLogger('botocore').setLevel(logging.DEBUG)
logging.getLogger('toshi_hazard_store').setLevel(logging.INFO)

formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
screen_handler = logging.StreamHandler(stream=sys.stdout)
screen_handler.setFormatter(formatter)
log.addHandler(screen_handler)


def columns_from_results(results):
    for res in results:
        levels = [val.lvl for val in res.values]
        poes = [val.val for val in res.values]
        yield (dict(lat=res.lat, lon=res.lon, vs30=res.vs30, agg=res.agg, imt=res.imt, apoe=poes, imtl=levels))


#  _ __ ___   __ _(_)_ __
# | '_ ` _ \ / _` | | '_ \
# | | | | | | (_| | | | | |
# |_| |_| |_|\__,_|_|_| |_|


@click.group()
def cli():
    """toshi_hazard_store cache utility - check, load, test."""
    pass
    # cache_info()


@cli.command()
def cache_info():
    """Get statistics about the local cache"""
    click.echo("Config settings from ENVIRONMENT")
    click.echo("--------------------------------")
    click.echo(f'LOCAL_CACHE_FOLDER: {LOCAL_CACHE_FOLDER}')
    click.echo(f'AWS REGION: {REGION}')
    click.echo(f'AWS DEPLOYMENT_STAGE: {DEPLOYMENT_STAGE}')

    click.echo("Available Aggregate values:")
    click.echo(ALL_AGG_VALS)

    click.echo("Available Intensity Measure Type (IMT) values:")
    click.echo(ALL_IMT_VALS)

    click.echo("Available VS30 values:")
    click.echo(ALL_VS30_VALS)

    click.echo("All City locations")
    click.echo(ALL_CITY_LOCS)


@cli.command()
@click.option('--num_locations', '-L', type=int, default=1)
@click.option('--num_imts', '-I', type=int, default=1)
@click.option('--num_vs30s', '-V', type=int, default=1)
@click.option('--num_aggs', '-A', type=int, default=1)
@click.option(
    '--model_id',
    '-M',
    default='NSHM_v1.0.4',
    type=click.Choice(['SLT_v8_gmm_v2_FINAL', 'SLT_v5_gmm_v0_SRWG', 'NSHM_1.0.0', 'NSHM_v1.0.4']),
)
def get_hazard_curves(model_id, num_aggs, num_vs30s, num_imts, num_locations):

    mHAG = model.HazardAggregation
    mHAG.create_table(wait=True)

    vs30s = ALL_VS30_VALS[:num_vs30s]
    imts = ALL_IMT_VALS[:num_imts]
    aggs = ALL_AGG_VALS[:num_aggs]
    locs = [loc.code for loc in ALL_CITY_LOCS[:num_locations]]

    count_cost_handler.reset()
    results = query.get_hazard_curves(locs, vs30s, [model_id], imts, aggs)
    pts_summary_data = pd.DataFrame.from_dict(columns_from_results(results))
    click.echo("get_hazard_curves Query consumed: %s units" % count_cost_handler.consumed)
    click.echo()

    click.echo(pts_summary_data.info())
    click.echo()
    click.echo(pts_summary_data.columns)
    click.echo()
    click.echo(pts_summary_data)
    click.echo()

    """
    ## BEFORE
    real    1m19.044s
    get_hazard_curves Query consumed: 88804.5 units

    ## AFTER
    real    0m4.601s
    get_hazard_curves Query consumed: 30.0 units

    ## speed / cost gains
    speed 79/4.6 = 17, cost 2970
    """


@cli.command()
@click.option('--location', '-L', type=str, default='MRO')
@click.option('--imt', '-I', type=str, default='PGA')
@click.option('--vs30', '-V', type=int, default=400)
@click.option('--agg', '-A', type=str, default='mean')
@click.option(
    '--model_id',
    '-M',
    default='NSHM_v1.0.4',
    type=click.Choice(['SLT_v8_gmm_v2_FINAL', 'SLT_v5_gmm_v0_SRWG', 'NSHM_1.0.0', 'NSHM_v1.0.4']),
)
def get_hazard_curve(model_id, agg, vs30, imt, location):

    mHAG = model.HazardAggregation
    mHAG.create_table(wait=True)

    vs30s = [
        vs30,
    ]
    imts = [
        imt,
    ]
    aggs = [agg]
    loc = location_by_id(location)
    locs = [
        CodedLocation(loc['latitude'], loc['longitude'], 0.001).code,
    ]
    print(loc, locs)

    count_cost_handler.reset()
    results = query.get_hazard_curves(locs, vs30s, [model_id], imts, aggs)
    pts_summary_data = pd.DataFrame.from_dict(columns_from_results(results))
    click.echo("get_hazard_curve Query consumed: %s units" % count_cost_handler.consumed)
    click.echo()

    click.echo(pts_summary_data.info())
    click.echo()
    click.echo(pts_summary_data.columns)
    click.echo()
    click.echo(pts_summary_data)
    click.echo()

    """
    ## BEFORE
    real    0m6.881s
    get_hazard_curve Query consumed: 7848.5 units

    ## AFTER
    real    0m1.727s
    get_hazard_curve Query consumed: 0.5 units


    ## speed / cost gains
    B) speed 6.8/1.7 = 4, cost 15697
    """


@cli.command()
@click.option('--many-query', '-Q', is_flag=True, show_default=True, default=False, help="use many query version")
@click.option('--imt', '-I', type=str, default='PGA')
@click.option('--vs30', '-V', type=int, default=400)
@click.option('--agg', '-A', type=str, default='0.995')
@click.option('--poe', '-P', type=float, default=0.02)
@click.option('--grid_id', '-G', type=str, default='NZ_0_1_NB_1_1')
@click.option(
    '--model_id',
    '-M',
    default='NSHM_v1.0.4',
    type=click.Choice(['SLT_v8_gmm_v2_FINAL', 'SLT_v5_gmm_v0_SRWG', 'NSHM_1.0.0', 'NSHM_v1.0.4']),
)
def get_gridded(many_query, model_id, grid_id, agg, vs30, imt, poe):
    count_cost_handler.reset()
    if not many_query:
        results = list(
            query.get_one_gridded_hazard(
                hazard_model_id=model_id,
                location_grid_id=grid_id,
                vs30=vs30,
                imt=imt,
                agg=agg,
                poe=poe,
            )
        )

        """
        get_one_gridded_hazard Query consumed: 0.5 units
        real    0m1.661s
        """

    else:
        results = list(
            query.get_gridded_hazard(
                hazard_model_ids=tuple([model_id]),
                location_grid_ids=tuple([grid_id]),
                vs30s=tuple([vs30]),
                imts=tuple([imt]),
                aggs=tuple([agg]),
                poes=tuple([poe]),
            )
        )

    click.echo("get_gridded_hazard Query consumed: %s units" % count_cost_handler.consumed)
    click.echo("Query returned: %s items" % len(results))

    """
    BEFORE
    get_gridded_hazard Query consumed: 6340.0 units
    real    0m5.317s

    AFTER
    get_gridded_hazard Query consumed: 0.5 units
    real    0m1.762s

    ## speed / cost gains
    speed 5.3/1.7 = 3.1, cost 6340/0.5 = 1260
    """


@cli.command()
@click.option('--location', '-L', type=str, default='MRO')
@click.option('--imt', '-I', type=str, default='PGA')
@click.option('--vs30', '-V', type=int, default=400)
@click.option('--agg', '-A', type=str, default='mean')
@click.option('--poe', '-P', type=float, default=2.0)
@click.option(
    '--model_id',
    '-M',
    default='NSHM_v1.0.4',
    type=click.Choice(['SLT_v8_gmm_v2_FINAL', 'SLT_v5_gmm_v0_SRWG', 'NSHM_1.0.0', 'NSHM_v1.0.4']),
)
# @click.option('--many-query', '-Q', is_flag=True, show_default=True, default=False, help="use many query version")
def get_disagg_agg_curve(model_id, agg, vs30, imt, poe, location):

    loc = location_by_id(location)
    locs = [
        CodedLocation(loc['latitude'], loc['longitude'], 0.001).code,
    ]
    print(loc, locs)
    count_cost_handler.reset()

    query.get_one_disagg_aggregation(
        model_id,
        model.AggregationEnum(agg),
        model.AggregationEnum(agg),
        CodedLocation(loc['latitude'], loc['longitude'], 0.001).code,
        vs30,
        imt,
        model.ProbabilityEnum._2_PCT_IN_50YRS,
    )
    click.echo("get_one_disagg_aggregation Query consumed: %s units" % count_cost_handler.consumed)

    """
    get_one_disagg_aggregation Query consumed: 1.5 units
    real    0m2.087s
    """


@cli.command()
@click.option('--num_locations', '-L', type=int, default=1)
@click.option('--num_imts', '-I', type=int, default=1)
@click.option('--num_vs30s', '-V', type=int, default=1)
@click.option('--num_aggs', '-A', type=int, default=1)
@click.option(
    '--model_id',
    '-M',
    default='NSHM_v1.0.4',
    type=click.Choice(['SLT_v8_gmm_v2_FINAL', 'SLT_v5_gmm_v0_SRWG', 'NSHM_1.0.0', 'NSHM_v1.0.4']),
)
def get_disagg_agg_curves(model_id, num_aggs, num_vs30s, num_imts, num_locations):

    vs30s = ALL_VS30_VALS[:num_vs30s]
    imts = ALL_IMT_VALS[:num_imts]
    aggs = ALL_AGG_VALS[:num_aggs]
    locs = [loc.code for loc in ALL_CITY_LOCS[:num_locations]]

    count_cost_handler.reset()
    results = query.get_hazard_curves(locs, vs30s, [model_id], imts, aggs)

    results = list(
        query.get_disagg_aggregates(
            [model_id],
            [model.AggregationEnum(agg) for agg in aggs],
            [model.AggregationEnum.MEAN],
            locs,
            vs30s,
            imts,
            [model.ProbabilityEnum._10_PCT_IN_50YRS],
        )
    )
    click.echo("get_disagg_agg_curves Query consumed: %s units" % count_cost_handler.consumed)
    click.echo("Query returned: %s items" % len(results))

    """
    BEFORE
    get_disagg_agg_curves Query consumed: 286.0 units
    real    0m1.670s

    AFTER
    get_disagg_agg_curves Query consumed: 0.5 units
    real    0m1.909s

    ## speed / cost gains
    speed 1.67/1.9 = 0.88, cost 286/0.5 = 572
    """


@cli.command()
@click.option('--num_locations', '-L', type=int, default=1)
def get_haz_api(num_locations):
    """Run Query typical of Kororaa Hazard curves view"""

    locs = [loc.code for loc in ALL_CITY_LOCS[:num_locations]]

    model_id = 'NSHM_v1.0.4'
    imts = ['PGA']
    # , 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)',
    #    'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)']
    # locs = ['-38.14~176.25']
    aggs = ['mean', '0.05', '0.95', '0.1', '0.9']
    vs30s = [1500]

    count_cost_handler.reset()
    results = list(query.get_hazard_curves(locs, vs30s, [model_id], imts, aggs))

    click.echo("get_hazard_curves Query consumed: %s units" % count_cost_handler.consumed)
    click.echo("Query returned: %s items" % len(results))
    """
    BEFORE: v0.7.2
    get_hazard_curves Query consumed: 9645.0 units
    real    0m9.023s

    AFTER: v0.7.3
    get_hazard_curves Query consumed: 40.0 units
    real    0m5.614s

    HACK:
    get_hazard_curves Query consumed: 153317.0 units
    real    1m46.624s
    """


@cli.command()
@click.option('--num_locations', '-L', type=int, default=1)
@click.option('--num_imts', '-I', type=int, default=1)
@click.option('--num_vs30s', '-V', type=int, default=1)
@click.option('--num_rlzs', '-R', type=int, default=1)
def get_rlzs(num_vs30s, num_imts, num_locations, num_rlzs):
    """Run Realizations query typical of Toshi Hazard Post"""
    vs30s = ALL_VS30_VALS[:num_vs30s]
    imts = ALL_IMT_VALS[:num_imts]
    # aggs = ALL_AGG_VALS[:num_aggs]
    rlzs = [n for n in range(6)][:num_rlzs]
    locs = [loc.code for loc in ALL_CITY_LOCS[:num_locations]]

    toshi_ids = ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==']
    # toshi_ids = ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODU2NQ==']
    count_cost_handler.reset()
    results = list(
        query.get_rlz_curves_v3(
            locs,
            vs30s,
            rlzs,
            toshi_ids,
            imts,
        )
    )
    # pts_summary_data = pd.DataFrame.from_dict(columns_from_results(results))

    click.echo(results[-1])
    click.echo("get_rlzs Query consumed: %s units" % count_cost_handler.consumed)
    click.echo("Query returned: %s items" % len(results))

    """
    BEFORE: v0.7.2
    get_rlzs Query consumed: 177412.5 units
    Query returned: 1 items

    real    2m9.078s

    AFTER: v0.7.3
    get_rlzs Query consumed: 1.5 units
    Query returned: 1 items

    real    0m1.647s
    """


@cli.command()
@click.option('--num_vs30s', '-V', type=int, default=1)
def get_meta(num_vs30s):
    """Run Meta query typical of Toshi Hazard Post"""
    vs30s = ALL_VS30_VALS[:num_vs30s]
    toshi_ids = ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==']
    # toshi_ids = ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODU2NQ==']

    count_cost_handler.reset()
    results = list(query_v3.get_hazard_metadata_v3(toshi_ids, vs30s))
    # pts_summary_data = pd.DataFrame.from_dict(columns_from_results(results))

    click.echo(results[-1])
    click.echo("get_rlzs Query consumed: %s units" % count_cost_handler.consumed)
    click.echo("Query returned: %s items" % len(results))

    """
    BEFORE: v0.7.4
    get_hazard_metadata_v3 Query consumed: 1229604.0 units
    Query returned: 1 items

    real    11m11.774s

    AFTER:
    THS_WIP_OpenquakeMeta-PROD<ToshiOpenquakeMeta, T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==:150>
    get_hazard_metadata_v3 Query consumed: 1.0 units
    Query returned: 1 items

    real    0m1.512s
    """


@cli.command()
def get_meta_38():
    """Run Meta query from THS issue #38"""
    # ref https://github.com/GNS-Science/toshi-hazard-store/issues/38
    vs30s = [225]
    toshi_ids = [
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYxMw==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU1MA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTMyMg==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU1MQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYwNA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTM3OA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU0Mg==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYxOQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU5Nw==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYxMA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTUyOA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU0OQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTM2MA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTMyMw==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYwMQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTM3Mw==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTQ2NQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTMyMQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTM2MQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTM2Nw==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU5NA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTM3MQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTM2OA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTY0Mw==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYwNw==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU0NQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYxMQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYwMA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTM2Mg==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYwNg==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYxMg==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU5NQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTQ2Nw==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU0OA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU5Ng==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU0NA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU0MQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYwOQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTUyOQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTU0MA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTQ3MQ==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTM3Mg==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYwOA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTUxOA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYxNg==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTUzMA==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTQ3Ng==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTQ2Ng==',
        'T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyOTYwNQ==',
    ]

    count_cost_handler.reset()
    results = list(query_v3.get_hazard_metadata_v3(toshi_ids, vs30s))
    # pts_summary_data = pd.DataFrame.from_dict(columns_from_results(results))

    click.echo(results[-1])
    click.echo("get_hazard_metadata_v3 Query consumed: %s units" % count_cost_handler.consumed)
    click.echo("Query returned: %s items" % len(results))

    """
    BEFORE: 0.7.4
    get_hazard_metadata_v3 Query consumed: 1229577.5 units
    Query returned: 49 items

    real    11m27.622s

    AFTER:
    get_rlzs Query consumed: 48.5 units
    Query returned: 49 items

    real    0m4.140s
    """


if __name__ == "__main__":
    cli()  # pragma: no cover
