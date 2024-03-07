"""Console script for loading openquake hazard to new REV4 tables."""

import logging
import pathlib
import sys
import datetime as dt
import click

try:
    from openquake.calculators.extract import Extractor
except (ModuleNotFoundError, ImportError):
    print("WARNING: the transform module uses the optional openquake dependencies - h5py, pandas and openquake.")
    raise

import toshi_hazard_store
from toshi_hazard_store.oq_import import export_rlzs_rev4


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

logging.basicConfig(level=logging.DEBUG)
logging.getLogger('pynamodb').setLevel(logging.DEBUG)
# logging.getLogger('botocore').setLevel(logging.DEBUG)
logging.getLogger('toshi_hazard_store').setLevel(logging.INFO)

formatter = logging.Formatter(fmt='%(asctime)s %(name)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
screen_handler = logging.StreamHandler(stream=sys.stdout)
screen_handler.setFormatter(formatter)
log.addHandler(screen_handler)

#  _ __ ___   __ _(_)_ __
# | '_ ` _ \ / _` | | '_ \
# | | | | | | (_| | | | | |
# |_| |_| |_|\__,_|_|_| |_|


@click.command()
@click.option(
    '--calc-id', '-CI', required=True, help='either an openquake calculation id OR filepath to the hdf5 file.'
)
@click.option('--compatible-calc-fk', '-CC', required=True, help='e.g. "hiktlck, b0.979, C3.9, s0.78"')
@click.option(
    '--producer-config-fk',
    '-PC',
    required=True,
    help='e.g. "SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEwODA3NQ==,RmlsZToxMDY1MjU="',
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
def cli(calc_id, compatible_calc_fk, producer_config_fk, hazard_calc_id, create_tables, verbose, dry_run):
    """store openquake hazard realizations to THS

    CALC_ID is either an openquake calculation id OR filepath to the hdf5 file.
    hazard_calc_id
    """

    hdf5_path = pathlib.Path(calc_id)
    if hdf5_path.exists():
        # we have a file path to work with
        extractor = Extractor(str(hdf5_path))
    else:
        calc_id = int(calc_id)
        extractor = Extractor(calc_id)

    if create_tables:
        if dry_run:
            click.echo('SKIP: Ensuring tables exist.')
        else:
            click.echo('Ensuring tables exist.')
            toshi_hazard_store.model.migrate_r4()
    if not dry_run:
        t0 = dt.datetime.utcnow()
        export_rlzs_rev4(
            extractor,
            compatible_calc_fk=compatible_calc_fk,
            producer_config_fk=producer_config_fk,
            vs30=400,
            return_rlz=False,
        )

        if verbose:
            t1 = dt.datetime.utcnow()
            click.echo("Done saving realisations, took %s secs" % (t1 - t0).total_seconds())
    else:
        click.echo('SKIP: saving realisations.')


if __name__ == "__main__":
    cli()  # pragma: no cover
