"""pyarrow helper function"""

import csv
import logging
import pathlib
import uuid
from functools import partial
from typing import TYPE_CHECKING, Iterable, Union

import pandas as pd
import pyarrow as pa
import pyarrow.dataset
import pyarrow.dataset as ds

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .hazard_aggregation import HazardAggregation
    from .hazard_realization_curve import HazardRealizationCurve


def write_metadata(output_folder: pathlib.Path, visited_file: pyarrow.dataset.WrittenFile) -> None:
    meta = [
        pathlib.Path(visited_file.path).relative_to(output_folder),
        visited_file.size,
    ]
    header_row = ["path", "size"]

    # NB metadata property does not exist for arrow format
    if visited_file.metadata:
        meta += [
            visited_file.metadata.format_version,
            visited_file.metadata.num_columns,
            visited_file.metadata.num_row_groups,
            visited_file.metadata.num_rows,
        ]
        header_row += ["format_version", "num_columns", "num_row_groups", "num_rows"]

    meta_path = pathlib.Path(visited_file.path).parent / "_metadata.csv"  # note prefix, otherwise parquet read fails
    write_header = False
    if not meta_path.exists():
        write_header = True
    with open(meta_path, 'a') as outfile:
        writer = csv.writer(outfile)
        if write_header:
            writer.writerow(header_row)
        writer.writerow(meta)
    log.debug(f"saved metadata to {meta_path}")


def append_models_to_dataset(
    models: Iterable[Union['HazardRealizationCurve', 'HazardAggregation']],
    output_folder: pathlib.Path,
    dataset_format: str = 'parquet',
) -> int:
    """
    append realisation models to dataset using the pyarrow library

    TODO: option to BAIL if realisation exists, assume this is a duplicated operation
    TODO: schema checks
    """

    def groomed_models(models):
        for model in models:
            yield model.as_pandas_model()

    df = pd.DataFrame(groomed_models(models))

    table = pa.Table.from_pandas(df)

    write_metadata_fn = partial(write_metadata, output_folder)

    ds.write_dataset(
        table,
        base_dir=str(output_folder),
        basename_template="%s-part-{i}.%s" % (uuid.uuid4(), dataset_format),
        partitioning=['nloc_0'],
        partitioning_flavor="hive",
        existing_data_behavior="overwrite_or_ignore",
        format=dataset_format,
        file_visitor=write_metadata_fn,
    )

    return df.shape[0]
