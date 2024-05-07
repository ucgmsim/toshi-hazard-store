"""The HazardRealizationCurve model

with support for model serialisation as pandas/pyarrow datasets
"""

import datetime as dt
import logging
import pathlib
import uuid
from functools import partial
from typing import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pytz
from nzshm_common.location.coded_location import CodedLocation
from pynamodb.attributes import ListAttribute, NumberAttribute, UnicodeAttribute
from pynamodb_attributes import TimestampAttribute

from toshi_hazard_store.config import DEPLOYMENT_STAGE, IS_OFFLINE, REGION

from ..attributes import EnumConstrainedUnicodeAttribute, ForeignKeyAttribute
from ..constraints import IntensityMeasureTypeEnum
from ..location_indexed_model import LocationIndexedModel, datetime_now
from .pyarrow_write_metadata import write_metadata

log = logging.getLogger(__name__)

VS30_KEYLEN = 4


class HazardRealizationCurve(LocationIndexedModel):
    """Stores hazard curve realizations."""

    # __metaclass__ = type

    class Meta:
        """DynamoDB Metadata."""

        billing_mode = 'PAY_PER_REQUEST'
        table_name = f"THS_R4_HazardRealizationCurve-{DEPLOYMENT_STAGE}"
        region = REGION
        if IS_OFFLINE:
            host = "http://localhost:8000"  # pragma: no cover

    partition_key = UnicodeAttribute(hash_key=True)  # a lot of these, let's look at our indexing
    sort_key = UnicodeAttribute(range_key=True)  # e.g ProducerID:MetaID

    compatible_calc_fk = ForeignKeyAttribute()
    sources_digest = UnicodeAttribute()
    gmms_digest = UnicodeAttribute()
    imt = EnumConstrainedUnicodeAttribute(IntensityMeasureTypeEnum)

    created = TimestampAttribute(default=datetime_now)
    producer_config_fk = ForeignKeyAttribute()  # attr_name="prod_conf_fk")

    values = ListAttribute(
        of=NumberAttribute
    )  # corresponding IMT levels are stored in the related HazardCurveProducerConfig

    # a reference to where/how this calc done (URI URL, http://nshm-blah-blah/api-ref
    calculation_id = UnicodeAttribute(null=True)

    def build_sort_key(self):
        vs30s = str(self.vs30).zfill(VS30_KEYLEN)
        sort_key = f'{self.nloc_001}:{vs30s}:{self.imt}:'
        sort_key += f'{ForeignKeyAttribute().serialize(self.compatible_calc_fk)}:'
        sort_key += self.sources_digest + ':'
        sort_key += self.gmms_digest
        return sort_key

    def set_location(self, location: CodedLocation):
        """Set internal fields, indices etc from the location."""
        LocationIndexedModel.set_location(self, location)
        # update the indices
        self.partition_key = self.nloc_1
        self.sort_key = self.build_sort_key()
        return self

    def as_pandas_model(self) -> dict:
        """
        Get the model ready for pandas serialisation
        """
        model = self.to_simple_dict()
        for fld in ['nloc_1', 'nloc_01', 'sort_key', 'partition_key', 'uniq_id']:
            del model[fld]
        model['created'] = dt.datetime.fromtimestamp(model['created'], pytz.timezone("UTC"))
        return model


def append_models_to_dataset(
    models: Iterable[HazardRealizationCurve], output_folder: pathlib.Path, dataset_format: str = 'parquet'
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
