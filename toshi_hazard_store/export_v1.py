from toshi_hazard_store import model, query
from toshi_hazard_store.transform import get_data


def export_stats(dstore, toshi_id: str, kind: str):
    oq = dstore['oqparam']
    curves = []
    for im in oq.imtls:
        for hazcurve in get_data(dstore, im, kind):
            # build the model objects....
            lvps = [
                model.LevelValuePairAttribute(lvl=float(x), val=float(y)) for (x, y) in zip(oq.imtls[im], hazcurve.poes)
            ]

            # strip the extra stuff
            agg = kind[9:] if "quantile-" in kind else kind
            obj = model.ToshiOpenquakeHazardCurveStats(
                loc=hazcurve.loc.site_code.decode(), imt=im, agg=agg, values=lvps
            )
            curves.append(obj)
            if len(curves) >= 50:
                query.batch_save_hcurve_stats(toshi_id, models=curves)
                curves = []

        # finally
        if len(curves):
            query.batch_save_hcurve_stats(toshi_id, models=curves)


def export_rlzs(dstore, toshi_id: str, kind: str):
    oq = dstore['oqparam']
    curves = []

    for im in oq.imtls:

        for hazcurve in get_data(dstore, im, kind):
            # build the model objects....
            lvps = [
                model.LevelValuePairAttribute(lvl=float(x), val=float(y)) for (x, y) in zip(oq.imtls[im], hazcurve.poes)
            ]

            # strip the extra stuff
            rlz = kind[4:] if "rlz-" in kind else kind
            obj = model.ToshiOpenquakeHazardCurveRlzs(loc=hazcurve.loc.site_code.decode(), imt=im, rlz=rlz, values=lvps)
            curves.append(obj)
            if len(curves) >= 50:
                query.batch_save_hcurve_rlzs(toshi_id, models=curves)
                curves = []

        # finally
        if len(curves):
            query.batch_save_hcurve_rlzs(toshi_id, models=curves)
