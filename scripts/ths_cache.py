"""Console script for testing or pre-poulating toshi_hazard_store local cache."""
# noqa
import logging
import os
import pathlib
import sys
import time

import click
import pandas as pd
from nzshm_common.grids import RegionGrid, load_grid
from nzshm_common.location.code_location import CodedLocation
from nzshm_common.location.location import LOCATION_LISTS, LOCATIONS, location_by_id

from toshi_hazard_store import model, query
from toshi_hazard_store.config import DEPLOYMENT_STAGE, LOCAL_CACHE_FOLDER, REGION

NZ_01_GRID = 'NZ_0_1_NB_1_1'

ALL_AGG_VALS = [e.value for e in model.AggregationEnum]
ALL_IMT_VALS = [e.value for e in model.IntensityMeasureTypeEnum]
ALL_VS30_VALS = [e.value for e in model.VS30Enum][1:]  # drop the 0 value!
ALL_CITY_LOCS = [CodedLocation(o['latitude'], o['longitude'], 0.001) for o in LOCATIONS]


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

pyconhandler = PyanamodbConsumedHandler(logging.DEBUG)
log.addHandler(pyconhandler)

# logging.basicConfig(level=logging.)
logging.getLogger('pynamodb').setLevel(logging.DEBUG)
# logging.getLogger('botocore').setLevel(logging.DEBUG)
# logging.getLogger('toshi_hazard_store').setLevel(logging.DEBUG)

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
@click.pass_context
def cache_info(ctx):
    """Get statistcics about the local cache"""
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
@click.option('--timing', '-T', is_flag=True, show_default=True, default=False, help="print timing information")
@click.option('--num_locations', '-L', type=int, default=5)
@click.option('--num_imts', '-I', type=int, default=5)
@click.option('--num_vs30s', '-V', type=int, default=5)
@click.option('--num_aggs', '-A', type=int, default=5)
@click.option(
    '--model_id',
    '-M',
    default='NSHM_1.0.2',
    type=click.Choice(['SLT_v8_gmm_v2_FINAL', 'SLT_v5_gmm_v0_SRWG', 'NSHM_1.0.0', 'NSHM_v1.0.4']),
)
@click.pass_context
def get_hazard_curves(ctx, model_id, num_aggs, num_vs30s, num_imts, num_locations, timing):

    mHAG = model.HazardAggregation
    mHAG.create_table(wait=True)

    vs30s = ALL_VS30_VALS[:num_vs30s]
    imts = ALL_IMT_VALS[:num_imts]
    aggs = ALL_AGG_VALS[:num_aggs]
    locs = [loc.code for loc in ALL_CITY_LOCS[:num_locations]]

    pyconhandler.reset()
    results = query.get_hazard_curves(locs, vs30s, [model_id], imts, aggs)
    pts_summary_data = pd.DataFrame.from_dict(columns_from_results(results))
    click.echo("get_hazard_curves Query consumed: %s units" % pyconhandler.consumed)
    click.echo()
    # for r in res:
    #     print(r)
    click.echo(pts_summary_data.info())
    click.echo()
    click.echo(pts_summary_data.columns)
    click.echo()
    click.echo(pts_summary_data)
    click.echo()
    # print(pts_summary_data['apoe'][0])
    # print(pts_summary_data['imtl'][0])

    # print(r.values)


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

    pyconhandler.reset()
    results = query.get_hazard_curves(locs, vs30s, [model_id], imts, aggs)
    pts_summary_data = pd.DataFrame.from_dict(columns_from_results(results))
    click.echo("get_hazard_curve Query consumed: %s units" % pyconhandler.consumed)
    click.echo()

    # for r in res:
    #     print(r)
    click.echo(pts_summary_data.info())
    click.echo()
    click.echo(pts_summary_data.columns)
    click.echo()
    click.echo(pts_summary_data)
    click.echo()
    # print(pts_summary_data['apoe'][0])
    # print(pts_summary_data['imtl'][0])

    # print(r.values)


@cli.command()
@click.option('--num_locations', '-L', type=int, default=5)
@click.pass_context
def get_annes_curves(ctx, num_locations):

    mHAG = model.HazardAggregation
    mHAG.create_table(wait=True)

    model_id = 'NSHM_v1.0.2'

    # setup locations
    id_list = LOCATION_LISTS['SRWG214']['locations']
    site_list = [location_by_id(loc_id)['name'] for loc_id in id_list]
    # site_list = ['Auckland', 'Wellington']
    id_list = [loc_id for loc_id in id_list if location_by_id(loc_id)['name'] in site_list]
    locations = [
        CodedLocation(location_by_id(loc_id)['latitude'], location_by_id(loc_id)['longitude'], 0.001)
        for loc_id in id_list
    ]
    # filter
    locs = [loc.code for loc in locations[:num_locations]]

    # setup other args
    vs30s = [150, 175, 225, 275, 375, 525, 750]
    imts = [
        'PGA',
        'SA(0.1)',
        'SA(0.15)',
        'SA(0.2)',
        'SA(0.25)',
        'SA(0.3)',
        'SA(0.35)',
        'SA(0.4)',
        'SA(0.5)',
        'SA(0.6)',
        'SA(0.7)',  #  'SA(0.8)', 'SA(0.9)', 'SA(1.0)', 'SA(1.25)', 'SA(1.5)', 'SA(1.75)', 'SA(2.0)', 'SA(2.5)',
        'SA(3.0)',
        'SA(3.5)',
        'SA(4.0)',
        'SA(4.5)',
        'SA(5.0)',
        'SA(6.0)',
        'SA(7.5)',
        'SA(10.0)',
    ]
    aggs = ["mean", "0.1", "0.5", "0.9"]

    # run the query
    results = query.get_hazard_curves(locs, vs30s, [model_id], imts, aggs)
    pts_summary_data = pd.DataFrame.from_dict(columns_from_results(results))

    # for r in res:
    #     print(r)
    click.echo(pts_summary_data.info())
    click.echo()
    click.echo(pts_summary_data.columns)
    click.echo()
    click.echo(pts_summary_data)
    click.echo()


@cli.command()
@click.option('--num_locations', '-L', type=int, default=5)
@click.option('--num_imts', '-I', type=int, default=5)
@click.option('--num_vs30s', '-V', type=int, default=5)
@click.option(
    '--model_id',
    '-M',
    default='SLT_v8_gmm_v2_FINAL',
    type=click.Choice(['SLT_v8_gmm_v2_FINAL', 'SLT_v5_gmm_v0_SRWG', 'NSHM_v1.0.2']),
)
@click.pass_context
def srwg_grid_curves(ctx, model_id, num_vs30s, num_imts, num_locations):

    mHAG = model.HazardAggregation
    mHAG.create_table(wait=True)

    site_list = 'NZ_0_1_NB_1_1'
    resample = 0.1
    locations = []
    grid = RegionGrid[site_list]
    grid_locs = grid.load()
    # remove empty location
    l = grid_locs.index((-34.7, 172.7))
    grid_locs = grid_locs[0:l] + grid_locs[l + 1 :]
    for gloc in grid_locs:
        loc = CodedLocation(*gloc, resolution=0.001)
        loc = loc.resample(float(resample)) if resample else loc
        locations.append(loc.resample(0.001))

    # filter
    locs = [loc.code for loc in locations[:num_locations]]

    # setup other args
    vs30s = [150, 175, 225, 275, 375, 525, 750][:num_vs30s]
    imts = [
        'PGA',
        'SA(0.1)',
        'SA(0.2)',
        'SA(0.3)',
        'SA(0.4)',
        'SA(0.5)',
        'SA(0.6)',
        'SA(0.7)',  #  'SA(0.8)', 'SA(0.9)', 'SA(1.0)', 'SA(1.25)', 'SA(1.5)', 'SA(1.75)', 'SA(2.0)', 'SA(2.5)',
        'SA(3.0)',
        'SA(4.0)',
        'SA(5.0)',
        'SA(6.0)',
        'SA(10.0)',
    ][:num_imts]
    """,
     # 'SA(0.15)',
    'SA(0.25)',
    'SA(0.35)',
    'SA(3.5)',
    'SA(4.5)',
    'SA(7.5)',
    """

    aggs = ["mean", "0.1", "0.5", "0.9"]

    # run the query
    results = query.get_hazard_curves(locs, vs30s, [model_id], imts, aggs)
    pts_summary_data = pd.DataFrame.from_dict(columns_from_results(results))

    # for r in res:
    #     print(r)
    click.echo(pts_summary_data.info())
    click.echo()
    click.echo(pts_summary_data.columns)
    click.echo()
    click.echo(pts_summary_data)
    click.echo()


@cli.command()
@click.pass_context
def add_site_vs30_col(ctx):

    from toshi_hazard_store import model
    from toshi_hazard_store.model.caching import execute_sql, get_connection, safe_table_name

    mHAG = model.HazardAggregation
    # mRLZ = model.OpenquakeRealization

    for model in [
        mHAG,
    ]:  #  mRLZ]:
        sql = """ALTER TABLE %s
            ADD COLUMN site_vs30 numeric;""" % safe_table_name(
            model
        )
        print(sql)

        conn = get_connection(model)
        res = execute_sql(conn, model, sql)
        print(res)


if __name__ == "__main__":
    cli()  # pragma: no cover
