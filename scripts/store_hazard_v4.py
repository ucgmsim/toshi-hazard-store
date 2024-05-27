"""Console script for loading openquake hazard to new REV4 tables.

WARNING:
 -  this module uses toshi_hazard_store.oq_import/export_rlzs_rev4 which exports to
    **DynamoDB** tables.
 - This may be what you want, but the direction we're heading is to export directly to **Parquet**.
   see scripts/ths_r4_import.py to see how parquet-direct works.

"""

import datetime as dt
import logging
import pathlib

import click

from toshi_hazard_store.model.revision_4 import hazard_models

try:
    from openquake.calculators.extract import Extractor
except (ModuleNotFoundError, ImportError):
    print("WARNING: the transform module uses the optional openquake dependencies - h5py, pandas and openquake.")
    raise

import toshi_hazard_store
from toshi_hazard_store.oq_import import (
    create_producer_config,
    export_rlzs_rev4,
    get_compatible_calc,
    get_producer_config,
)

log = logging.getLogger()

logging.basicConfig(level=logging.INFO)
logging.getLogger('pynamodb').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)


def get_extractor(calc_id: str):
    """return an extractor for given calc_id or path to hdf5"""
    hdf5_path = pathlib.Path(calc_id)
    try:
        if hdf5_path.exists():
            # we have a file path to work with
            extractor = Extractor(str(hdf5_path))
        else:
            extractor = Extractor(int(calc_id))
    except Exception as err:
        log.info(err)
        return None
    return extractor


#  _ __ ___   __ _(_)_ __
# | '_ ` _ \ / _` | | '_ \
# | | | | | | (_| | | | | |
# |_| |_| |_|\__,_|_|_| |_|
@click.group()
def main():
    pass


@main.command()
@click.option('--partition', '-P', required=True, help="partition key")
@click.option('--uniq', '-U', required=False, default=None, help="uniq_id, if not specified a UUID will be used")
@click.option('--notes', '-N', required=False, default=None, help="optional notes about the this calc compatability")
@click.option('-c', '--create-tables', is_flag=True, default=False, help="Ensure tables exist.")
@click.option(
    '-d',
    '--dry-run',
    is_flag=True,
    default=False,
    help="dont actually do anything.",
)
def compat(partition, uniq, notes, create_tables, dry_run):
    """create a new hazard calculation compatability identifier"""

    mCHC = hazard_models.CompatibleHazardCalculation
    if create_tables:
        if dry_run:
            click.echo('SKIP: Ensuring tables exist.')
        else:
            click.echo('Ensuring tables exist.')
            toshi_hazard_store.model.migrate_r4()

    t0 = dt.datetime.utcnow()
    if uniq:
        m = mCHC(partition_key=partition, uniq_id=uniq, notes=notes)
    else:
        m = mCHC(partition_key=partition, notes=notes)

    if not dry_run:
        m.save()
        t1 = dt.datetime.utcnow()
        click.echo("Done saving CompatibleHazardCalculation, took %s secs" % (t1 - t0).total_seconds())
    else:
        click.echo('SKIP: saving CompatibleHazardCalculation.')


@main.command()
@click.option('--partition', '-P', required=True, help="partition key")
@click.option('--compatible-calc-fk', '-CC', required=True, help="foreign key of the compatible_calc in form `A_B`")
@click.option(
    '--calc-id',
    '-CI',
    required=False,
    help='either an openquake calculation id OR filepath to the hdf5 file. Used to obtain IMTs and levels',
)
@click.option('--software', '-S', required=True, help="name of the producer software")
@click.option('--version', '-V', required=True, help="version of the producer software")
@click.option('--hashed', '-H', required=True, help="hash of the producer configuration")
@click.option('--config', '-C', required=False, help="producer configuration as a unicode string")
@click.option('--notes', '-N', required=False, help="user notes")
@click.option('-c', '--create-tables', is_flag=True, default=False, help="Ensure tables exist.")
@click.option(
    '-v',
    '--verbose',
    is_flag=True,
    default=False,
    help="Increase output verbosity.",
)
@click.option(
    '-d',
    '--dry-run',
    is_flag=True,
    default=False,
    help="dont actually do anything.",
)
def producer(
    partition, compatible_calc_fk, calc_id, software, version, hashed, config, notes, create_tables, verbose, dry_run
):
    """create a new hazard producer config. May use calc-id to get template IMT and IMT_LEVELS"""

    extractor = get_extractor(calc_id)

    compatible_calc = get_compatible_calc(compatible_calc_fk.split("_"))
    if compatible_calc is None:
        raise ValueError(f'compatible_calc: {compatible_calc.foreign_key()} was not found')

    model = create_producer_config(
        partition_key=partition,
        compatible_calc=compatible_calc,
        extractor=extractor,
        producer_software=software,
        producer_version_id=version,
        configuration_hash=hashed,
        configuration_data=config,
        notes=notes,
        dry_run=dry_run,
    )
    if verbose:
        click.echo(f"Model {model} has foreign key ({model.partition_key}, {model.range_key})")


@main.command()
@click.option(
    '--calc-id', '-CI', required=True, help='either an openquake calculation id OR filepath to the hdf5 file.'
)
@click.option(
    '--compatible-calc-fk',
    '-CC',
    required=True,
    # help='e.g. "hiktlck, b0.979, C3.9, s0.78"'
)
@click.option(
    '--producer-config-fk',
    '-PC',
    required=True,
    # help='e.g. "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwODA3NQ==,RmlsZToxMDY1MjU="',
)
@click.option('--hazard_calc_id', '-H', help='hazard_solution id.')
@click.option('-c', '--create-tables', is_flag=True, default=False, help="Ensure tables exist.")
@click.option(
    '-v',
    '--verbose',
    is_flag=True,
    default=False,
    help="Increase output verbosity.",
)
@click.option(
    '-d',
    '--dry-run',
    is_flag=True,
    default=False,
    help="dont actually do anything.",
)
def rlz(calc_id, compatible_calc_fk, producer_config_fk, hazard_calc_id, create_tables, verbose, dry_run):
    """store openquake hazard revision 4 realizations to THS"""

    if create_tables:
        if dry_run:
            click.echo('SKIP: Ensuring tables exist.')
        else:
            click.echo('Ensuring tables exist.')
            toshi_hazard_store.model.migrate_r4()

    hdf5_path = pathlib.Path(calc_id)
    if hdf5_path.exists():
        # we have a file path to work with
        extractor = Extractor(str(hdf5_path))
    else:
        calc_id = int(calc_id)
        extractor = Extractor(calc_id)

    compatible_calc = get_compatible_calc(compatible_calc_fk.split("_"))
    if compatible_calc is None:
        click.echo(f'compatible_calc: {compatible_calc_fk} was not found. Load failed')
        return

    producer_config = get_producer_config(producer_config_fk.split("_"), compatible_calc)
    if producer_config is None:
        click.echo(f'producer_config {producer_config_fk} was not found. Load failed')
        return

    if not dry_run:
        t0 = dt.datetime.utcnow()
        export_rlzs_rev4(
            extractor,
            compatible_calc=compatible_calc,
            producer_config=producer_config,
            hazard_calc_id=hazard_calc_id,
            vs30=400,
            return_rlz=False,
        )

        if verbose:
            t1 = dt.datetime.utcnow()
            click.echo("Done saving realisations, took %s secs" % (t1 - t0).total_seconds())
    else:
        click.echo('SKIP: saving realisations.')


if __name__ == "__main__":
    main()
