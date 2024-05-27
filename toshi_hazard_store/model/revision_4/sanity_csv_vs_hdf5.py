import json
import pathlib

import numpy as np
import pandas as pd
from openquake.calculators.extract import Extractor

WORKING = pathlib.Path('/GNSDATA/LIB/toshi-hazard-store/WORKING/CLASSIC')


def reshape_csv_curve_rlz_dataframe(df1):
    collapse_cols = [cname for cname in df1.columns if 'poe' in cname]

    def generate_subtables(df1, collapse_cols):
        for idx, key in enumerate(collapse_cols):
            drop_cols = collapse_cols.copy()
            drop_cols.remove(key)
            sub_df = df1.drop(columns=drop_cols)
            yield sub_df.rename(columns={key: "poe"})

    return pd.concat(generate_subtables(df1, collapse_cols))


def df_from_csv(rlz_idx: int = 0, imt_label: str = 'PGA'):
    csv_file = (
        WORKING
        / 'openquake_csv_archive-T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDYw'
        / f'hazard_curve-rlz-{rlz_idx:03d}-{imt_label}_1.csv'
    )
    df_csv = pd.read_csv(str(csv_file), header=1)
    return reshape_csv_curve_rlz_dataframe(df_csv)


lat, lon = '-34.500~173.000'.split('~')
site_idx = 1950
# rlz_idx = 20

# HDF5
hdf5_file = WORKING / 'calc_1.hdf5'
extractor = Extractor(str(hdf5_file))

# source_lt, gsim_lt, rlz_lt = parse_logic_tree_branches(extractor)
# print(rlz_lt)
# rlz_lt = pd.DataFrame(extractor.dstore['full_lt'].rlzs
# assert 0

oqparam = json.loads(extractor.get('oqparam').json)
# sites = extractor.get('sitecol').to_dframe()
#

### OLD => OK, only up to SA(2.0)
oq = extractor.dstore['oqparam']  # old way
imtls = oq.imtls  # dict of imt and the levels used at each imt e.g {'PGA': [0.011. 0.222]}
imtl_keys = list(oq.imtls.keys())

'''
# NEW => BAD :
imtls = oqparam['hazard_imtls']
imtl_keys = list(imtls.keys())
'''

# SA(10.0)
mystery_array_26 = np.asarray(
    [
        2.6296526e-02,
        1.5997410e-02,
        8.9979414e-03,
        6.1928276e-03,
        4.6614003e-03,
        3.6940516e-03,
        1.6577756e-03,
        6.4969447e-04,
        3.5134773e-04,
        2.2066629e-04,
        1.5147004e-04,
        4.3425865e-05,
        1.0680247e-05,
        4.1670401e-06,
        1.9728300e-06,
        1.0438350e-06,
        9.7031517e-08,
        1.7055431e-08,
        4.0719232e-09,
        1.1564985e-09,
        3.6237868e-10,
        1.1791490e-10,
        3.7686188e-11,
        1.1331824e-11,
        3.5563774e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
        1.6076029e-12,
    ]
)

mystery_array = np.asarray(
    [
        6.0450632e-02,
        6.0432829e-02,
        6.0144477e-02,
        5.9362564e-02,
        5.8155395e-02,
        5.6671314e-02,
        4.8372149e-02,
        3.5934746e-02,
        2.8352180e-02,
        2.3324875e-02,
        1.9734636e-02,
        1.0642946e-02,
        4.7865356e-03,
        2.7201117e-03,
        1.7424060e-03,
        1.2033664e-03,
        3.3416378e-04,
        1.4450523e-04,
        7.6706347e-05,
        4.5886023e-05,
        2.9674735e-05,
        2.0267133e-05,
        1.4408529e-05,
        1.0562427e-05,
        7.9324709e-06,
        4.7287931e-06,
        2.9796386e-06,
        1.9564620e-06,
        1.3266620e-06,
        9.2331248e-07,
        6.5663625e-07,
        4.7568375e-07,
        3.5006093e-07,
        2.6118445e-07,
        1.9726333e-07,
        1.0229679e-07,
        5.5962094e-08,
        3.1938363e-08,
        1.8840048e-08,
        7.0585950e-09,
        2.8224134e-09,
        1.1749444e-09,
        4.9472115e-10,
        2.0887614e-10,
    ]
)

# NEWER most efficeint way
# 23 secs
# rlzs = extractor.get('hcurves?kind=rlzs', asdict=True)

imtl_keys = sorted(imtl_keys)
print('sorted imtl_keys', imtl_keys)
# assert 0

# for imt_label, rlz_idx in itertools.product(imtl_keys, rlz_indices):
rlz_indices = range(21)
for rlz_idx in rlz_indices:

    for imt_label in imtl_keys:
        imt_idx = imtl_keys.index(imt_label)

        # CDC suggestion, use imt in query string
        rlzs = extractor.get(f'hcurves?kind=rlzs&imt={imt_label}', asdict=True)
        hdf5_values = rlzs[f'rlz-{rlz_idx:03d}'][site_idx][0]

        # print(rlzs.keys())
        # print(rlzs[f'rlz-{rlz_idx:03d}'].shape)
        # assert 0

        # GET data from 3D array
        # NEW WAY (works only if imt_labels are sorted
        '''
        hdf5_values = rlzs[f'rlz-{rlz_idx:03d}'][site_idx][imt_idx]
        '''

        # # OLD WAY
        # old_hdf5_values=extractor.dstore['hcurves-rlzs'][site_idx][rlz_idx][imt_idx]
        # assert np.allclose(old_hdf5_values, hdf5_values)

        # CSV numpy
        df_csv = df_from_csv(rlz_idx=rlz_idx, imt_label=imt_label)
        flt = (df_csv.lon == float(lon)) & (df_csv.lat == float(lat))
        csv_values = df_csv[flt]['poe'].to_numpy()

        # # NEEDLE & haystack APPROACH...

        # # NEEDLE & haystack APPROACH...
        # if np.allclose(csv_values, mystery_array):
        #     print(f'found match for mystery array rlz-{rlz_idx:03d}, {imt_label} with index {imt_idx}')
        #     assert 0
        #     continue

        # if np.allclose(csv_values, mystery_array_26): # SA(10.0)
        #     print(f'found match for mystery array 26 rlz-{rlz_idx:03d}, {imt_label} with index {imt_idx}')
        #     # assert 0
        #     continue

        # # allow checking to continue
        # if np.allclose(hdf5_values, mystery_array_26):
        #     print('SKIP after {imt_label} with index {imt_idx} as we found mystery_array_26')
        #     # assert 0
        #     continue

        # compare the numpy way
        if not np.allclose(csv_values, hdf5_values):
            print(f'theyre OFF for rlz-{rlz_idx:03d}, {imt_label} with index {imt_idx}')
            # continue
            print('csv_values')
            print('==========')
            print(csv_values)
            print()
            print('hdf5_values')
            print('===========')
            print(hdf5_values)
            assert 0
        else:
            print(f'theyre close for rlz-{rlz_idx:03d}, {imt_label} with index {imt_idx}')
