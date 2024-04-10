# r4_to_arrow.py
# flake8: noqa
# mypy: ignore-errors
import datetime

import pyarrow as pa
import pyarrow.dataset as ds

from toshi_hazard_store.model.revision_4 import hazard_models

sample = hazard_models.HazardRealizationCurve(
    created=datetime.datetime(2024, 4, 4, 4, 22, 25, tzinfo=datetime.timezone.utc),
    compatible_calc_fk=('A', 'A'),
    producer_config_fk=(
        'A',
        '461564345538.dkr.ecr.us-east-1.amazonaws.com/nzshm22/runzi-openquake:8c09bffb9f4cf88bbcc9:bdc5476361cd',
    ),
    calculation_id='T3BlbnF1YWtlSGF6YXJkU29sdXRpb246MTMyODUxNw==',
    values=[
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
        0.0,
    ],
    imt='PGA',
    vs30=275,
    site_vs30=None,
    sources_digest='0a89f830786d',
    gmms_digest='e031e948959c',
    nloc_001='-46.100~166.400',
    nloc_01='-46.10~166.40',
    nloc_1='-46.1~166.4',
    nloc_0='-46.0~166.0',
    lat=-46.1,
    lon=166.4,
    uniq_id='1308a94d-bcd8-4fca-adac-52f637dfa5dc',
    partition_key='-46.1~166.4',
    sort_key='-46.100~166.400:0275:PGA:A_A:s0a89f830786d:ge031e948959c',
)


hrc_schema = pa.schema(
    [
        ('created', pa.timestamp('ms', tz='UTC')),
        ('compatible_calc_fk', pa.string()),
        ('producer_config_fk', pa.string()),
        ('calculation_id', pa.string()),
        ('values', pa.list_(pa.float32(), 43)),
        ('imt', pa.string()),
        ('vs30', pa.uint16()),
        ('site_vs30', pa.uint16()),
        ('source_digests', pa.list_(pa.string(), -1)),
        ('gmm_digests', pa.list_(pa.string(), -1)),
        ('nloc_001', pa.string()),
        ('partition_key', pa.string()),
        ('sort_key', pa.string()),
    ]
)

# import numpy.random
# data = pa.table({"day": numpy.random.randint(1, 31, size=100),
#                  "month": numpy.random.randint(1, 12, size=100),
#                  "year": [2000 + x // 10 for x in range(100)]})

# print(data)
# print()
# ds.write_dataset(data, "./partitioned", format="parquet",
#                  partitioning=ds.partitioning(pa.schema([("year", pa.int16())])))


def hazard_realization_curve(rlz):
    """Do the thing"""


# import pandas as pd
# print(vars(sample))
# print()

# def chunked(iterable, chunk_size=100):
#   count = 0
#   chunk = []
#   for item in iterable:
#       chunk.append(item)
#       count +=1
#       if count % chunk_size == 0:
#           yield chunk
#           chunk = []

#   if chunk:
#       yield chunk


# for chunk in chunked(data, 50):
#   df = pd.DataFrame(chunk)
#   print(df)


# print(sample.to_simple_dict())

'''
import pyarrow.dataset as ds
import pyarrow.compute as pc
dataset = ds.dataset('WORKING/ARROW/pq-t2.4', format='parquet')
#t0 = dataset.to_table()

>>> dataset.count_rows()
3085785

# with fully_populated dataset

dataset = ds.dataset('WORKING/ARROW/pq-t2.4/nloc=-39.0~176.0', format='parquet')
flt = (pc.field('imt')==pc.scalar("PGA")) & (pc.field("nloc_001")==pc.scalar("-39.000~175.930"))
'''

# dataset.head(10, filter=(pc.field('imt')==pc.scalar("IMT"))
# dataset.head(10, filter=(pc.field('imt')==pc.scalar("PGA")), columns=['values','vs30','nloc_001'])
# col2_sum = 0
# count = 0
# for batch in dataset.to_batches(columns=["col2"], filter=~ds.field("col2").is_null()):
#     col2_sum += pc.sum(batch.column("col2")).as_py()
#     count += batch.num_rows


# mean_a = col2_sum/count

import json

import numpy as np
from openquake.calculators.extract import Extractor

from toshi_hazard_store.oq_import.parse_oq_realizations import build_rlz_mapper
from toshi_hazard_store.utils import normalise_site_code

# extractor = Extractor('WORKING/R2VuZXJhbFRhc2s6MTMyODQxNA==/subtasks/T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDc0/calc_1.hdf5')
extractor = Extractor('WORKING/R2VuZXJhbFRhc2s6MTMyODQxNA==/subtasks/T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NTA0/calc_1.hdf5')
oq = json.loads(extractor.get('oqparam').json)
sites = extractor.get('sitecol').to_dframe()
rlzs = extractor.get('hcurves?kind=rlzs', asdict=True)

rlz_keys = [k for k in rlzs.keys() if 'rlz-' in k]
imtls = oq['hazard_imtls']  # dict of imt and the levels used at each imt e.g {'PGA': [0.011. 0.222]}
rlz_map = build_rlz_mapper(extractor)


# for i_imt, imt in enumerate(imtls.keys()):
#     print(i_imt, imt)

site_codes = [
    normalise_site_code((sites.loc[i_site, 'lon'], sites.loc[i_site, 'lat']), True).code for i_site in range(len(sites))
]
rlzs_col = np.repeat(list(rlz_map.keys()), len(imtls.keys()) * len(site_codes))
sites_col = np.tile(np.repeat(site_codes, len(imtls.keys())), len(rlz_map.keys()))
imts_col = np.tile(list(imtls.keys()), len(rlz_map.keys()) * len(site_codes))

# Values
all_vals = None
# print(all_vals.shape)
# print(all_vals)
# assert 0
for i_rlz in rlz_map.keys():
    # print(rlzs[rlz_keys[i_rlz]].shape)
    new_vals = pa.array(np.reshape(rlzs[rlz_keys[i_rlz]], (len(imtls.keys()) * len(site_codes), 44)))
    assert 0
    print(new_vals.shape)
    print(new_vals)
    # assert 0
    if all_vals is None:
        all_vals = np.copy(new_vals)
    else:
        all_vals = np.append(all_vals, new_vals, axis=0)
print(all_vals.shape)

# assert 0
# print (site_codes)
print('rlzs', len(rlzs_col), len(rlz_map), rlzs_col[:45])
print('sites', len(sites_col), len(sites), sites_col[:20])
print('imts', len(imts_col), len(imtls), imts_col[:45])

table = pa.table(dict(site=sites_col, rlz=rlzs_col, imt=imts_col, values=all_vals))

df = table.to_pandas()
print(df)
print(df.loc[100])

assert 0

site_col, rlz_col, imt_col, values_col = [], [], [], []

# print(f'loc: {loc}')
for i_rlz in rlz_map.keys():
    for i_site in range(len(sites)):
        loc = normalise_site_code((sites.loc[i_site, 'lon'], sites.loc[i_site, 'lat']), True)
        # source_branch, gmm_branch = bp.split('~')
        for i_imt, imt in enumerate(imtls.keys()):
            site_col.append(loc.code)
            rlz_col.append(i_rlz)
            imt_col.append(imt)
            # values_col.append(rlzs[rlz_keys[i_rlz]][i_site][i_imt].tolist())

table2 = pa.table(dict(site=site_col, rlz=rlz_col, imt=imt_col))

df2 = table2.to_pandas()

print(df2)
print(df2.loc[100])
assert 0

# for i_site in range(len(sites)):
#             loc = normalise_site_code((sites.loc[i_site, 'lon'], sites.loc[i_site, 'lat']), True)
#             print(f'loc: {loc}')
#             for i_rlz in rlz_map.keys():
#                 # source_branch, gmm_branch = bp.split('~')
#                 for i_imt, imt in enumerate(imtls.keys()):
#                     values = rlzs[rlz_keys[i_rlz]][i_site][i_imt]
#                     print(values.shape, i_imt, i_rlz, i_site)
#                     assert 0
