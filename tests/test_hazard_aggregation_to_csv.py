"""test csv export from HazardAggregation."""

import csv
import io

from toshi_hazard_store import query_v3


class TestQueryHazardAggregationV3Csv:
    def test_query_and_serialise_csv(self, build_hazagg_models, adapted_hazagg_model, many_hazagg_args):

        qlocs = [loc.downsample(0.001).code for loc in many_hazagg_args['locs'][:2]]
        res = list(
            query_v3.get_hazard_curves(
                locs=qlocs,
                vs30s=many_hazagg_args['vs30s'],
                hazard_model_ids=[many_hazagg_args['HAZARD_MODEL_ID']],
                imts=many_hazagg_args['imts'],
                # model=adapted_hazagg_model,
            )
        )

        csv_file = io.StringIO()
        writer = csv.writer(csv_file)
        writer.writerows(adapted_hazagg_model.HazardAggregation.to_csv(res))

        csv_file.seek(0)
        header = next(csv_file)

        print(header)
        # assert 0
        rows = list(itm for itm in csv_file)
        # assert header.startswith('agg,imt,lat,lon,vs30,poe-')
        assert len(res) == len(rows)
        assert [str(rv.val) for rv in res[-1].values[-10:]] == rows[-1].strip().split(',')[
            -10:
        ]  # last 10 vals in the last row
