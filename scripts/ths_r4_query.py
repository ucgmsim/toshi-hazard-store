"""Console script for querying THS_R4 tables
"""

import logging

import click

log = logging.getLogger()

logging.basicConfig(level=logging.INFO)
logging.getLogger('pynamodb').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('toshi_hazard_store').setLevel(logging.INFO)

# import nzshm_model  # noqa: E402
import toshi_hazard_store  # noqa: E402

# from toshi_hazard_store.model.revision_4 import hazard_models  # noqa: E402

# from toshi_hazard_store.config import (
#     USE_SQLITE_ADAPTER,
#     LOCAL_CACHE_FOLDER,
#     DEPLOYMENT_STAGE as THS_STAGE,
#     REGION as THS_REGION,
# )


#  _ __ ___   __ _(_)_ __
# | '_ ` _ \ / _` | | '_ \
# | | | | | | (_| | | | | |
# |_| |_| |_|\__,_|_|_| |_|


@click.group()
@click.pass_context
def main(context):
    """Import NSHM Model hazard curves to new revision 4 models."""

    context.ensure_object(dict)
    # context.obj['work_folder'] = work_folder


@main.command()
@click.argument('partition')
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
@click.pass_context
def lsc(context, partition, verbose, dry_run):
    for compat in toshi_hazard_store.model.CompatibleHazardCalculation.query(partition):
        click.echo(compat)


@main.command()
@click.argument('partition')
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
@click.pass_context
def lsp(context, partition, verbose, dry_run):
    """list HazardCurveProducerConfig in PARTITION"""

    results = list(toshi_hazard_store.model.HazardCurveProducerConfig.query(partition))
    for pc in sorted(results, key=lambda x: x.effective_from):
        row = [
            pc.partition_key,
            pc.range_key,
            "_".join(pc.compatible_calc_fk),
            str(pc.effective_from),
            str(pc.last_used),
            pc.tags,
            pc.configuration_hash,
            pc.notes,
        ]
        click.echo(row)


if __name__ == "__main__":
    main()
