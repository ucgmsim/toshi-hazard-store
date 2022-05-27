# Usage

To use toshi-hazard-store in a project


```
from toshi_hazard_store import query

## get some solution meta data ...
for m in query.get_hazard_metadata(None, vs30_vals=[250, 350]) #get all solution meta with given VS values
	print(m.vs30, m.hazard_solution_id)


## get some agreggate curves
for r in query.get_hazard_stats_curves(m.hazard_solution_id, 'PGA', ['WLG', 'QZN', 'CHC', 'DUD'], ['mean']):
	print(r.loc_code, r.lvl_val_pairs[0])


## get some realisation curves
for r in query.get_hazard_rlz_curves(m.hazard_solution_id, 'PGA', ['WLG', 'QZN', 'CHC', 'DUD']):
    print(r.loc_code, r.rlz_id, r.lvl_val_pairs[0] )
```

