The NZSHM toshi-hazard-store database is available for public, read-only access using AWS API credentials (contact via email: nshm@gns.cri.nz).

### Environment & Authorisation pre-requisites

``` console
NZSHM22_HAZARD_STORE_STAGE=XXXX (TEST or PROD)
NZSHM22_HAZARD_STORE_REGION=XXXXX (ap-southeast-2)
AWS_PROFILE- ... (See AWS authentication below)

```

#### AWS Authentication

 - AWS credientials will be provided with so-called `short-term credentials` in the form of an `awx_access_key_id` and and `aws_access_key_secret`.

 - Typically these are configured in your local credentials file as described in [Authenticate with short-term credentials](https://docs.aws.amazon.com/cli/v1/userguide/cli-authentication-short-term.html).

 - An `AWS_PROFILE` environment variable determines the credentials used at run-time by THS.

## toshi-hazard-store (library)

To use toshi-hazard-store in a project


```
from toshi_hazard_store import query
import pandas as pd
import  json

TOSHI_ID = "abcdef"

## get some solution meta data ...
for m in query.get_hazard_metadata(None, vs30_vals=[250, 350]):
    print(m.vs30, m.haz_sol_id, m.locs)

    source_lt = pd.read_json(m.src_lt)
    gsim_lt = pd.read_json(m.gsim_lt)

    rlzs_df = pd.read_json(m.rlz_lt) # realizations meta as pandas datframe.
    rlzs_dict = json.loads(m.rlz_lt) # realizations meta as dict.

    print(rlzs_dict)
    print(rlzs_df)


## get some agreggate curves
for r in query.get_hazard_stats_curves(m.haz_sol_id, ['PGA'], ['WLG', 'QZN', 'CHC', 'DUD'], ['mean']):
    print("stat", r.loc, r.values[0])
    break

## get some realisation curves
for r in query.get_hazard_rlz_curves(m.haz_sol_id, ['PGA'], ['WLG', 'QZN', 'CHC', 'DUD']):
    print("rlz", r.loc, r.rlz, r.values[0] )
    break



```

## store_hazard (script)

TODO decribe usage of the upload script
