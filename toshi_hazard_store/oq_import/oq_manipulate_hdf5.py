"""
NSHM 2022 used GSIMs that were not included in openquake version < 3.19.

Some of these got different names in openquake. This module will rewrite an HDF5 calulation file produced with
outdated names into those recognised by oq 3.19. Additional arguments are added in some cases.

NB maybe this belongs in the nzshm_model.psha_adapter.openquake package ??

"""

import collections
import logging
import pathlib

import h5py

log = logging.getLogger(__name__)

GsimRow = collections.namedtuple("GsimRow", "region, key, uncertainty, weight")


def migrate_nshm_uncertainty_string(uncertainty: str) -> str:
    # handle GMM modifications ...
    if "[Atkinson2022" in uncertainty:
        uncertainty += '\nmodified_sigma = "true"'
    elif "[AbrahamsonGulerce2020S" in uncertainty:
        uncertainty = uncertainty.replace("AbrahamsonGulerce2020S", "NZNSHM2022_AbrahamsonGulerce2020S")
    elif "[KuehnEtAl2020S" in uncertainty:
        uncertainty = uncertainty.replace("KuehnEtAl2020S", "NZNSHM2022_KuehnEtAl2020S")
        uncertainty += '\nmodified_sigma = "true"'
    elif "[ParkerEtAl2021" in uncertainty:
        uncertainty = uncertainty.replace("ParkerEtAl2021", "NZNSHM2022_ParkerEtAl2020")
        uncertainty += '\nmodified_sigma = "true"'
    return uncertainty


def migrate_gsim_row(row: GsimRow) -> GsimRow:
    log.debug(f"Manipulating row {row}")
    new_row = GsimRow(
        region=row.region,
        key=row.key,
        uncertainty=migrate_nshm_uncertainty_string(row.uncertainty.decode()).encode(),
        weight=row.weight,
    )
    log.debug(f"New value: {row}")
    return new_row


def rewrite_calc_gsims(hdf5_path: pathlib.Path):
    """NSHM specifc modifictions for old HDF5 file

    Modify the GSIM attributes to conform with standard openquake from 3.19 and
    with NSHM identity strings.

    Arguments:
        filepath: path to the hdf5 file to be manipulated.
    """
    log.info(f"Manipulating {hdf5_path} file")
    # hdf5_path = pathlib.Path(filepath)
    if not hdf5_path.exists():
        raise ValueError(f"The file was not found: {hdf5_path}")

    hdf5_file = h5py.File(str(hdf5_path), 'r+')
    dataset = hdf5_file['full_lt']['gsim_lt']

    for idx, row in enumerate(dataset):
        dataset[idx] = migrate_gsim_row(GsimRow(*row))

    hdf5_file.close()
