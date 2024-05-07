from .hazard_aggregate_curve import HazardAggregateCurve
from .hazard_aggregate_curve import drop_tables as drop_ha
from .hazard_aggregate_curve import migrate as migrate_ha
from .hazard_models import CompatibleHazardCalculation, HazardCurveProducerConfig
from .hazard_models import drop_tables as drop_hm  # HazardRealizationMeta,
from .hazard_models import migrate as migrate_hm
from .hazard_realization_curve import HazardRealizationCurve
from .hazard_realization_curve import drop_tables as drop_hrc
from .hazard_realization_curve import migrate as migrate_hrc


def migrate():
    migrate_hm()
    migrate_hrc()
    migrate_ha()


def drop_tables():
    drop_hm()
    drop_hrc()
    drop_ha()
