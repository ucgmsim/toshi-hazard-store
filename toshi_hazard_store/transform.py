"""Helper functions to export an openquake calculation and save it with toshi-hazard-store.

Courtesy of Anne Hulsey
"""

import re
from collections import namedtuple

import numpy as np
import pandas as pd

CustomLocation = namedtuple("CustomLocation", "site_code lon lat")
CustomHazardCurve = namedtuple("CustomHazardCurve", "loc poes")


def parse_logic_tree_branches(extractor):

    full_lt = extractor.get('full_lt')
    source_model_lt = full_lt.source_model_lt
    utypes = {bs.id: bs.uncertainty_type for bs in source_model_lt.branchsets}
    source_lt = pd.DataFrame(
        {
            'branch': source_model_lt.branches.keys(),
            'branchset': [b.bs_id for b in source_model_lt.branches.values()],
            'utype': [utypes[b.bs_id] for b in source_model_lt.branches.values()],
            # double quote for backwards compatability
            'uvalue': ["'" + b.value + "'" for b in source_model_lt.branches.values()],
            'weight': [b.weight for b in source_model_lt.branches.values()],
            'branch_code': [b.id for b in source_model_lt.branches.values()],
        }
    )
    source_lt.set_index('branch_code', inplace=True)

    gslt = full_lt.gsim_lt
    gsim_lt = pd.DataFrame(
        {
            'trt': [b.trt for b in gslt.branches],
            'branch': [b.id for b in gslt.branches],
            'uncertainty': [str(b.gsim) for b in gslt.branches],
            'weight': [b.weight.dic['weight'] for b in gslt.branches],
        }
    )
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
                print(f'{tags[1]}_{tags[3]}')
                df.loc[j, 'model name'] = f'{tags[1]}_{tags[3]}'
            else:
                df.loc[j, 'model name'] = tags[1]
        gsim_lt_dict[i] = df

    full_lt.rlzs
    rlz_lt = pd.DataFrame(columns=['branch_path', 'weight'], data=[[rlz[1], rlz[2]] for rlz in full_lt.rlzs])
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
