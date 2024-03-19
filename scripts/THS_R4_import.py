"""Console script for preparing to load NSHM hazard curves to new REV4 tables using General Task(s) and nzshm-model.

This is NSHM process specific, as it assumes the following:
 - hazard producer metadata is available from the NSHM toshi-api via **nshm-toshi-client** library
 - NSHM model characteristics are available in the **nzshm-model** library

Hazard curves are store using the new THS Rev4 tables which may also be used independently.

Given a general task containing hazard calcs used in NHSM, we want to iterate over the sub-tasks and do the setup required
for importing the hazard curves:

    - pull the configs and check we have a compatible producer config (or ...) cmd `producers`
    - optionally create new producer configs automatically, and record info about these
       - NB if new producer configs are created, then it is the users responsibility to assign a CompatibleCalculation to each

These things may get a separate script
    - OPTION to download HDF5 and load hazard curves from there
    - OPTION to import V3 hazard curves from DynamodDB and extract ex
"""

import datetime as dt
import logging
import os
import pathlib
import nzshm_model

import click

from toshi_hazard_store.model.revision_4 import hazard_models

try:
    from openquake.calculators.extract import Extractor
except (ModuleNotFoundError, ImportError):
    print("WARNING: the transform module uses the optional openquake dependencies - h5py, pandas and openquake.")
    raise

import toshi_hazard_store
from toshi_hazard_store.oq_import import (
    #create_producer_config,
    #export_rlzs_rev4,
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

@click.group()
@click.option('--work_folder', '-W', default=lambda: os.getcwd(), help="defaults to Current Working Directory")
@click.pass_context
def main(context, work_folder):
    """Import NSHM Model hazard curves to new revision 4 models."""

    context.ensure_object(dict)
    context.obj['work_folder'] = work_folder


@main.command()
@click.argument('model_id')  # , '-M', default="NSHM_v1.0.4")
@click.argument('gt_id')
@click.argument('partition')
@click.option(
    '--compatible_calc_fk',
    '-CCF',
    default="A_A",
    required=True,
    help="foreign key of the compatible_calc in form `A_B`",
)
@click.option(
    '--create_new',
    '-C',
    is_flag=True,
    default=False,
    help="if false, then bail, otherwise create a new producer record.",
)
# @click.option('--software', '-S', required=True, help="name of the producer software")
# @click.option('--version', '-V', required=True, help="version of the producer software")
# @click.option('--hashed', '-H', required=True, help="hash of the producer configuration")
# @click.option('--config', '-C', required=False, help="producer configuration as a unicode string")
# @click.option('--notes', '-N', required=False, help="user notes")
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
@click.pass_context
def producers(
    context,
    model_id,
    gt_id,
    partition,
    compatible_calc_fk,
    create_new,
    # software, version, hashed, config, notes,
    verbose,
    dry_run,
):
    """Prepare and validate Producer Configs for a given MODEL_ID and GT_ID in a PARTITION

    MODEL_ID is a valid NSHM model identifier\n
    GT_ID is an NSHM General task id containing HazardAutomation Tasks\n
    PARTITION is a table partition (hash)

    Notes:\n
    - pull the configs and check we have a compatible producer config\n
    - optionally, create any new producer configs
    """

    work_folder = context.obj['work_folder']
    current_model = nzshm_model.get_model_version(model_id)

    if verbose:
        click.echo(f"using verbose: {verbose}")
        click.echo(f"using work_folder: {work_folder}")
        click.echo(f"using model_id: {current_model.version}")
        click.echo(f"using gt_id: {gt_id}")
        click.echo(f"using partition: {partition}")

    # slt = current_model.source_logic_tree()

    # extractor = get_extractor(calc_id)

    compatible_calc = get_compatible_calc(compatible_calc_fk.split("_"))
    if compatible_calc is None:
        raise ValueError(f'compatible_calc: {compatible_calc.foreign_key()} was not found')

    # model = create_producer_config(
    #     partition_key=partition,
    #     compatible_calc=compatible_calc,
    #     extractor=extractor,
    #     producer_software=software,
    #     producer_version_id=version,
    #     configuration_hash=hashed,
    #     configuration_data=config,
    #     notes=notes,
    #     dry_run=dry_run,
    # )
    # if verbose:
    #     click.echo(f"Model {model} has foreign key ({model.partition_key}, {model.range_key})")


if __name__ == "__main__":
    main()
