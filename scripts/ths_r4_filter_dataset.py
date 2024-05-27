# flake8: noqa
"""
Console script for filtering from existing THS parquet dataset, producing a smaller one.
"""

import csv
import datetime as dt
import logging
import os
import pathlib
import uuid
from functools import partial

# import time
import click
import pyarrow as pa

# import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow.dataset as ds
from nzshm_common.location import coded_location, location

# import pandas as pd


log = logging.getLogger(__name__)

logging.basicConfig(level=logging.INFO)

ANNES_12_SWRG_LOCS = [
    'Auckland',
    'Blenheim',
    'Christchurch',
    'Dunedin',
    'Gisborne',
    'Greymouth',
    'Masterton',
    'Napier',
    'Nelson',
    'Queenstown',
    'Tauranga',
    'Wellington',
]


@click.command()
@click.argument('source')
@click.argument('target')
@click.option(
    '-L',
    '--locations',
    help="one or more location identifiers (comma-separated). Use any valid nzshm_location identifier",
)
@click.option('-VS', '--vs30s', help="one or more vs30 identifiers (comma-separated). Use any valid NSHM VS30")
@click.option('-v', '--verbose', is_flag=True, default=False)
@click.option('-d', '--dry-run', is_flag=True, default=False)
def main(
    source,
    target,
    locations,
    vs30s,
    verbose,
    dry_run,
):
    """Filter realisations dataset within each loc0 partition"""
    source_folder = pathlib.Path(source)
    target_folder = pathlib.Path(target)
    target_parent = target_folder.parent

    assert source_folder.exists(), f'source {source_folder} is not found'
    assert source_folder.is_dir(), f'source {source_folder} is not a directory'

    assert target_parent.exists(), f'folder {target_parent} is not found'
    assert target_parent.is_dir(), f'folder {target_parent} is not a directory'

    DATASET_FORMAT = 'parquet'  # TODO: make this an argument
    BAIL_AFTER = 0  # 0 => don't bail

    # resolve bins from locations
    # TODO: the following code requires knowledge of location internals, Here we're trying to solve two issues
    # A: match the location by name istead of code
    # B: get names from SRG locations nto NZ or NZ2 ( are these 12 locations different coords in the SWRG214 and NZ lists)?
    #
    # Question -  how should we support this , seems a bit error prone??
    if not locations:
        locations = [
            loc['id'] for loc in location.LOCATIONS if (loc['name'] in ANNES_12_SWRG_LOCS and loc['id'][:3] == "srg")
        ]
        user_locations = location.get_locations(locations)
    else:
        user_locations = location.get_locations(locations.split(","))

    partition_bins = coded_location.bin_locations(user_locations, 1.0)
    dataset = ds.dataset(source_folder, format=DATASET_FORMAT, partitioning='hive')

    if not len(user_locations) < 200:
        assert 0, "possibly we can't process big lists this way"

    tables = []
    for partition_code, partition_bin in partition_bins.items():
        for loc in partition_bin.locations:
            flt0 = (pc.field('nloc_0') == pc.scalar(partition_code)) & (pc.field('nloc_001') == pc.scalar(loc.code))

        print(flt0)
        arrow_scanner = ds.Scanner.from_dataset(dataset, filter=flt0)
        tables.append(arrow_scanner.to_table())

    arrow_tables = pa.concat_tables(tables)

    # writemeta_fn = partial(write_metadata, target_folder)
    ds.write_dataset(
        arrow_tables,
        base_dir=str(target_folder),
        basename_template="%s-part-{i}.%s" % (uuid.uuid4(), DATASET_FORMAT),
        partitioning=['nloc_0'],  # TODO: make this an argument
        partitioning_flavor="hive",
        # existing_data_behavior="delete_matching",
        format=DATASET_FORMAT,
        # file_visitor=writemeta_fn,
    )

    click.echo(f'filter {len(user_locations)} locations to {target_folder.parent}')


if __name__ == "__main__":
    main()
