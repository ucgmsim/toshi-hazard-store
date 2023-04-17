"""Helper functions to export an openquake calculation and save it with toshi-hazard-store."""

import re
from collections import namedtuple

import numpy as np

CustomLocation = namedtuple("CustomLocation", "site_code lon lat")
CustomHazardCurve = namedtuple("CustomHazardCurve", "loc poes")

try:
    import h5py
    import pandas as pd
    from openquake.baselib.general import BASE183

    # from openquake.calculators.export.hazard import extract, get_sites
    from openquake.commonlib import datastore
except (Exception) as err:
    print("WARNING: the transform module uses the optional openquake dependencies - h5py, pandas and openquake.")
    print(err)
    raise err


def parse_logic_tree_branches(file_id):
    """Extract the dataframes."""

    with h5py.File(file_id) as hf:
        # read and prepare the source model logic tree for documentation
        ### full_lt is a key that contains subkeys for each type of logic tree
        ### here we read the contents of source_model_lt into a dataframe
        source_lt = pd.DataFrame(hf['full_lt']['source_model_lt'][:])
        for col in source_lt.columns[:-1]:
            source_lt.loc[:, col] = source_lt[col].str.decode('ascii')

        # identify the source labels used in the realizations table
        source_lt.loc[:, 'branch_code'] = [x for x in BASE183[0 : len(source_lt)]]
        source_lt.set_index('branch_code', inplace=True)

        # read and prepare the gsim logic tree for documentation
        ### full_lt is a key that contains subkeys for each type of logic tree
        ### here we read the contents of gsim_lt into a dataframe
        gsim_lt = pd.DataFrame(hf['full_lt']['gsim_lt'][:])
        for col in gsim_lt.columns[:-1]:
            gsim_lt.loc[:, col] = gsim_lt.loc[:, col].str.decode('ascii')

        # break up the gsim df into tectonic regions (one df per column of gsims in realization labels. e.g. A~AAA)
        # the order of the dictionary is consistent with the order of the columns
        gsim_lt_dict = {}
        for i, trt in enumerate(np.unique(gsim_lt['trt'])):
            df = gsim_lt[gsim_lt['trt'] == trt]
            df.loc[:, 'branch_code'] = [x[1] for x in df['branch']]
            df.set_index('branch_code', inplace=True)
            ### the branch code used to be a user specified string from the gsim logic tree .xml
            ### now the only way to identify which regionalization is used is to extract it manually
            for j, x in zip(df.index, df['uncertainty']):
                tags = re.split('\\[|\\]|\nregion = \"|\"', x)
                if len(tags) > 4:
                    df.loc[j, 'model name'] = f'{tags[1]}_{tags[3]}'
                else:
                    df.loc[j, 'model name'] = tags[1]
            gsim_lt_dict[i] = df

    # read and prep the realization record for documentation
    ### this one can be read into a df directly from the dstore's full_lt
    ### the column titled 'ordinal' is dropped, as it will be the same as the 0-n index
    dstore = datastore.read(file_id)
    rlz_lt = pd.DataFrame(dstore['full_lt'].rlzs).drop('ordinal', axis=1)

    # add to the rlt_lt to note which source models and which gsims were used for each branch
    for i_rlz in rlz_lt.index:
        # rlz name is in the form A~AAA, with a single source identifier followed by characters for each trt region
        srm_code, gsim_codes = rlz_lt.loc[i_rlz, 'branch_path'].split('~')

        # copy over the source label
        rlz_lt.loc[i_rlz, 'source combination'] = source_lt.loc[srm_code, 'branch']

        # loop through the characters for the trt region and add the corresponding gsim name
        for i, gsim_code in enumerate(gsim_codes):
            trt, gsim = gsim_lt_dict[i].loc[gsim_code, ['trt', 'model name']]
            rlz_lt.loc[i_rlz, trt] = gsim

    return source_lt, gsim_lt, rlz_lt
