"""Console script for testing DBAdapter vs PyanamodbConsumedHandler"""

# noqa
import logging
import sys

import click
import pandas as pd
from nzshm_common.location.coded_location import CodedLocation
from nzshm_common.location.location import LOCATIONS, location_by_id

# Monkey-patch temporary
import toshi_hazard_store.query.hazard_query
from toshi_hazard_store import configure_adapter, model, query
from toshi_hazard_store.db_adapter.sqlite import SqliteAdapter

# toshi_hazard_store.query.hazard_query.model = model
# toshi_hazard_store.query.hazard_query.mRLZ = model.OpenquakeRealization

NZ_01_GRID = 'NZ_0_1_NB_1_1'

ALL_AGG_VALS = [e.value for e in model.AggregationEnum]
ALL_IMT_VALS = [e.value for e in model.IntensityMeasureTypeEnum]
ALL_VS30_VALS = [e.value for e in model.VS30Enum][1:]  # drop the 0 value!
ALL_CITY_LOCS = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in LOCATIONS]

configure_adapter(adapter_model=SqliteAdapter)


class PyanamodbConsumedHandler(logging.Handler):
    def __init__(self, level=0) -> None:
        super().__init__(level)
        self.consumed = 0

    def reset(self):
        self.consumed = 0

    def emit(self, record):
        if "pynamodb/connection/base.py" in record.pathname and record.msg == "%s %s consumed %s units":
            self.consumed += record.args[2]
            # print("CONSUMED:",  self.consumed)


log = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)
count_cost_handler = PyanamodbConsumedHandler(logging.DEBUG)
log.addHandler(count_cost_handler)
formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
screen_handler = logging.StreamHandler(stream=sys.stdout)
screen_handler.setFormatter(formatter)
log.addHandler(screen_handler)

log.debug('DEBUG message')
log.info('INFO message')


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
@click.option('--timing', '-T', is_flag=True, show_default=True, default=False, help="print timing information")
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
@click.pass_context
def get_hazard_curve(ctx, model_id, agg, vs30, imt, location, timing):

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
    results = toshi_hazard_store.query.get_hazard_curves(locs, vs30s, [model_id], imts, aggs)
    pts_summary_data = pd.DataFrame.from_dict(columns_from_results(results))
    click.echo("get_hazard_curve Query consumed: %s units" % count_cost_handler.consumed)
    click.echo()

    # for r in res:
    #     print(r)
    click.echo(pts_summary_data.info())
    click.echo()
    click.echo(pts_summary_data.columns)
    click.echo()
    click.echo(pts_summary_data)
    click.echo()


@cli.command()
@click.option('--num_locations', '-L', type=int, default=1)
@click.option('--num_imts', '-I', type=int, default=1)
@click.option('--num_vs30s', '-V', type=int, default=1)
@click.option('--num_rlzs', '-R', type=int, default=1)
def get_rlzs(num_vs30s, num_imts, num_locations, num_rlzs):
    """Run Realizations query typical of Toshi Hazard Post"""

    # vs30s = ALL_VS30_VALS[:num_vs30s]
    vs30s = [150]
    imts = ALL_IMT_VALS[:num_imts]
    rlzs = [n for n in range(6)][:num_rlzs]

    # locs = [loc.code for loc in ALL_CITY_LOCS[:num_locations]]
    o = location_by_id('IVC')
    locs = [
        CodedLocation(o['latitude'], o['longitude'], 0.001).code,
    ]

    toshi_ids = ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODMzNg==']
    # toshi_ids = ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTA2ODU2NQ==']
    # toshi_ids = ['T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODcwMQ==']
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

    for r in results:
        click.echo(r)
    click.echo("get_rlzs Query consumed: %s units" % count_cost_handler.consumed)
    click.echo("Query returned: %s items" % len(results))


@cli.command()
def get_adapter():
    mHAG = model.OpenquakeRealization

    # now query
    o = location_by_id('IVC')
    loc = CodedLocation(o['latitude'], o['longitude'], 0.1)
    print(loc)
    hash_key = loc.code  # '-43.2~177.3'
    # range_condition = model.OpenquakeRealization.sort_key >= '-43.200~177.270:000:PGA'
    # filter_condition = mHAG.vs30.is_in(0) & mHAG.imt.is_in('PGA') & mHAG.hazard_model_id.is_in('HAZ_MODEL_ONE')

    m2 = next(
        mHAG.query(
            hash_key=hash_key,
            range_key_condition=model.OpenquakeRealization.sort_key
            >= "-46.400~168.400:400:000000:T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODcwMQ==",
            # filter_condition=filter_condition,
        )
    )
    print(m2)


if __name__ == "__main__":
    cli()  # pragma: no cover
