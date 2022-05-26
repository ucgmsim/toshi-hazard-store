# Usage

To use toshi-hazard-store in a project

```

from toshi_hazard_store import model, query

TOSHI_ID = "FASDASDSAD" # A real Toshi ID is necesaary of course


res = query.get_hazard_curves_stats(TOSHI_ID, 250, 'PGA', ['WLG', 'QZN', 'CHC', 'DUD'], ['mean'])
for r in res:
	print(r)

res = query.get_hazard_curves_stats(TOSHI_ID, 250, 'SA(0.5)', None, ['mean'])
for r in res:
    print(r)

```
