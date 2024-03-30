import json
import logging
import pathlib
import zipfile

import requests
from nzshm_model.psha_adapter.openquake.hazard_config import OpenquakeConfig
from nzshm_model.psha_adapter.openquake.hazard_config_compat import DEFAULT_HAZARD_CONFIG

# from typing import Dict


log = logging.getLogger(__name__)

ARCHIVED_INI = "archived_job.ini"
SYNTHETIC_INI = 'synthetic_job.ini'
TASK_ARGS_JSON = "task_args.json"


def save_file(filepath: pathlib.Path, url: str):
    r = requests.get(url, stream=True)
    if r.ok:
        with open(filepath, 'wb') as f:
            f.write(r.content)
        return filepath
    else:
        raise (RuntimeError(f'Error downloading file {filepath.name}: Status code {r.status_code}'))


def download_artefacts(gtapi, task_id, hazard_task_detail, subtasks_folder, include_hdf5=False):
    """Pull down the files and store localling in WORKFOLDER"""

    subtask_folder = subtasks_folder / str(task_id)
    subtask_folder.mkdir(exist_ok=True)

    save_file(subtask_folder / TASK_ARGS_JSON, hazard_task_detail['hazard_solution']['task_args']['file_url'])

    if include_hdf5:
        hdf5_file = subtask_folder / "calc_1.hdf5"
        if not hdf5_file.exists():
            hazard_task_detail['hazard_solution']['hdf5_archive']['file_name']
            hdf5_archive = save_file(
                subtask_folder / hazard_task_detail['hazard_solution']['hdf5_archive']['file_name'],
                hazard_task_detail['hazard_solution']['hdf5_archive']['file_url'],
            )

            # TODO handle possibly different filename ??
            with zipfile.ZipFile(hdf5_archive) as myzip:
                myzip.extract('calc_1.hdf5', subtask_folder)
            hdf5_archive.unlink()  # delete the zip


def hdf5_from_task(task_id, subtasks_folder):
    """Use nzshm-model to build a compatibility config"""
    subtask_folder = subtasks_folder / str(task_id)
    hdf5_file = subtask_folder / "calc_1.hdf5"
    assert hdf5_file.exists()
    return hdf5_file


def config_from_task(task_id, subtasks_folder) -> OpenquakeConfig:
    """Use nzshm-model to build a compatibility config"""
    subtask_folder = subtasks_folder / str(task_id)
    ta = json.load(open(subtask_folder / TASK_ARGS_JSON, 'r'))

    if ta.get("oq"):
        log.info('new-skool config')
        config = OpenquakeConfig(ta.get("oq"))
    else:
        log.info('old-skool config')
        config = (
            OpenquakeConfig(DEFAULT_HAZARD_CONFIG)
            .set_parameter("erf", "rupture_mesh_spacing", str(ta['rupture_mesh_spacing']))
            .set_parameter("general", "ps_grid_spacing", str(ta["ps_grid_spacing"]))
        )

    # both old and new-skool get these args from top-level of task_args
    config.set_description(SYNTHETIC_INI).set_vs30(ta['vs30']).set_iml(
        ta['intensity_spec']['measures'], ta['intensity_spec']['levels']
    )
    with open(subtask_folder / SYNTHETIC_INI, 'w') as f:
        config.write(f)

    return config

    # check_hashes(task_id, config)


new_skool_example = {
    'general': {'random_seed': 25, 'calculation_mode': 'classical', 'ps_grid_spacing': 30},
    'logic_tree': {'number_of_logic_tree_samples': 0},
    'erf': {
        'rupture_mesh_spacing': 4,
        'width_of_mfd_bin': 0.1,
        'complex_fault_mesh_spacing': 10.0,
        'area_source_discretization': 10.0,
    },
    'site_params': {'reference_vs30_type': 'measured'},
    'calculation': {
        'investigation_time': 1.0,
        'truncation_level': 4,
        'maximum_distance': {'Active Shallow Crust': '[[4.0, 0], [5.0, 100.0], [6.0, 200.0], [9.5, 300.0]]'},
    },
    'output': {'individual_curves': 'true'},
}

old_skool_example = {
    'config_archive_id': 'RmlsZToxMjkxNjk4',
    'model_type': 'COMPOSITE',
    'logic_tree_permutations': [
        {
            'tag': 'GRANULAR',
            'weight': 1.0,
            'permute': [
                {
                    'group': 'ALL',
                    'members': [
                        {
                            'tag': 'geodetic, TI, N2.7, b0.823 C4.2 s1.41',
                            'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyOTE1MDI=',
                            'bg_id': 'RmlsZToxMzA3MTM=',
                            'weight': 1.0,
                        }
                    ],
                }
            ],
        }
    ],
    'intensity_spec': {
        'tag': 'fixed',
        'measures': [
            'PGA',
            'SA(0.1)',
            'SA(0.2)',
            'SA(0.3)',
            'SA(0.4)',
            'SA(0.5)',
            'SA(0.7)',
            'SA(1.0)',
            'SA(1.5)',
            'SA(2.0)',
            'SA(3.0)',
            'SA(4.0)',
            'SA(5.0)',
            'SA(6.0)',
            'SA(7.5)',
            'SA(10.0)',
            'SA(0.15)',
            'SA(0.25)',
            'SA(0.35)',
            'SA(0.6)',
            'SA(0.8)',
            'SA(0.9)',
            'SA(1.25)',
            'SA(1.75)',
            'SA(2.5)',
            'SA(3.5)',
            'SA(4.5)',
        ],
        'levels': [
            0.0001,
            0.0002,
            0.0004,
            0.0006,
            0.0008,
            0.001,
            0.002,
            0.004,
            0.006,
            0.008,
            0.01,
            0.02,
            0.04,
            0.06,
            0.08,
            0.1,
            0.2,
            0.3,
            0.4,
            0.5,
            0.6,
            0.7,
            0.8,
            0.9,
            1.0,
            1.2,
            1.4,
            1.6,
            1.8,
            2.0,
            2.2,
            2.4,
            2.6,
            2.8,
            3.0,
            3.5,
            4,
            4.5,
            5.0,
            6.0,
            7.0,
            8.0,
            9.0,
            10.0,
        ],
    },
    'vs30': 275,
    'location_list': ['NZ', 'NZ_0_1_NB_1_1', 'SRWG214'],
    'disagg_conf': {'enabled': False, 'config': {}},
    'rupture_mesh_spacing': 4,
    'ps_grid_spacing': 30,
    'split_source_branches': False,
}

"""
INFO:scripts.revision_4.oq_config:old-skool config
INFO:scripts.revision_4.oq_config:{'config_archive_id': 'RmlsZToxMjkxNjk4', 'model_type': 'COMPOSITE', 'logic_tree_permutations': [{'tag': 'GRANULAR', 'weight': 1.0, 'permute': [{'group': 'ALL', 'members': [{'tag': 'geodetic, TI, N2.7, b0.823 C4.2 s1.41', 'inv_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyOTE1MDI=', 'bg_id': 'RmlsZToxMzA3MTM=', 'weight': 1.0}]}]}], 'intensity_spec': {'tag': 'fixed', 'measures': ['PGA', 'SA(0.1)', 'SA(0.2)', 'SA(0.3)', 'SA(0.4)', 'SA(0.5)', 'SA(0.7)', 'SA(1.0)', 'SA(1.5)', 'SA(2.0)', 'SA(3.0)', 'SA(4.0)', 'SA(5.0)', 'SA(6.0)', 'SA(7.5)', 'SA(10.0)', 'SA(0.15)', 'SA(0.25)', 'SA(0.35)', 'SA(0.6)', 'SA(0.8)', 'SA(0.9)', 'SA(1.25)', 'SA(1.75)', 'SA(2.5)', 'SA(3.5)', 'SA(4.5)'], 'levels': [0.0001, 0.0002, 0.0004, 0.0006, 0.0008, 0.001, 0.002, 0.004, 0.006, 0.008, 0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4, 4.5, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]}, 'vs30': 275, 'location_list': ['NZ', 'NZ_0_1_NB_1_1', 'SRWG214'], 'disagg_conf': {'enabled': False, 'config': {}}, 'rupture_mesh_spacing': 4, 'ps_grid_spacing': 30, 'split_source_branches': False}
chrisbc@tryharder-ubuntu:/GNSDATA/LIB/toshi-hazard-store$ poetry run ths_r4_import -W WORKING producers NSHM_v1.0.4 R2VuZXJhbFRhc2s6NjcwMTI1NA== A -CCF A_A
INFO:botocore.credentials:Found credentials in shared credentials file: ~/.aws/credentials
INFO:botocore.credentials:Found credentials in shared credentials file: ~/.aws/credentials
INFO:pynamodb.settings:Override settings for pynamo available /etc/pynamodb/global_default_settings.py
INFO:toshi_hazard_store.model:Configure adapter: <class 'toshi_hazard_store.db_adapter.sqlite.sqlite_adapter.SqliteAdapter'>
INFO:botocore.credentials:Found credentials in shared credentials file: ~/.aws/credentials
INFO:scripts.revision_4.oq_config:new-skool config
INFO:scripts.revision_4.oq_config:{'title': 'OpenQuake Hazard Calcs', 'description': 'Logic Tree 9.0.1, locations for cave locations', 'task_type': 'HAZARD', 'gmcm_logic_tree': "<?xml version=``1.0`` encoding=``UTF-8``?>--<nrml xmlns:gml=``http://www.opengis.net/gml``-      xmlns=``http://openquake.org/xmlns/nrml/0.4``>-    <logicTree logicTreeID='lt1'>-            <logicTreeBranchSet uncertaintyType=``gmpeModel`` branchSetID=``bs_crust``-                    applyToTectonicRegionType=``Active Shallow Crust``>--                    <logicTreeBranch branchID=``BSSA2014_center``>-                <uncertaintyModel>[BooreEtAl2014]-                  sigma_mu_epsilon = 0.0 </uncertaintyModel>-                        <uncertaintyWeight>1.0</uncertaintyWeight>-                    </logicTreeBranch>--            </logicTreeBranchSet>--            <logicTreeBranchSet uncertaintyType=``gmpeModel`` branchSetID=``bs_interface``-                    applyToTectonicRegionType=``Subduction Interface``>--                <logicTreeBranch branchID=``ATK22_SI_center``>-                    <uncertaintyModel>[Atkinson2022SInter]-                          epistemic = ``Central``-                            modified_sigma = ``true``-                           </uncertaintyModel>-                    <uncertaintyWeight>1.0</uncertaintyWeight>-                      </logicTreeBranch>--            </logicTreeBranchSet>--            <logicTreeBranchSet uncertaintyType=``gmpeModel`` branchSetID=``bs_slab``-                    applyToTectonicRegionType=``Subduction Intraslab``>--                <logicTreeBranch branchID=``ATK22_SS_center``>-                      <uncertaintyModel>[Atkinson2022SSlab]-                              epistemic = ``Central``-                              modified_sigma = ``true``-                              </uncertaintyModel>-                        <uncertaintyWeight>1.0</uncertaintyWeight>-                </logicTreeBranch>--            </logicTreeBranchSet>-    </logicTree>-</nrml>-", 'model_type': 'COMPOSITE', 'intensity_spec': {'tag': 'fixed', 'measures': ['PGA'], 'levels': [0.01, 0.02, 0.04, 0.06, 0.08, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0, 3.5, 4.0, 4.5, 5.0]}, 'location_list': ['WLG', 'AKL', 'DUD', 'CHC'], 'vs30': 400, 'disagg_conf': {'enabled': False, 'config': {}}, 'oq': {'general': {'random_seed': 25, 'calculation_mode': 'classical', 'ps_grid_spacing': 30}, 'logic_tree': {'number_of_logic_tree_samples': 0}, 'erf': {'rupture_mesh_spacing': 4, 'width_of_mfd_bin': 0.1, 'complex_fault_mesh_spacing': 10.0, 'area_source_discretization': 10.0}, 'site_params': {'reference_vs30_type': 'measured'}, 'calculation': {'investigation_time': 1.0, 'truncation_level': 4, 'maximum_distance': {'Active Shallow Crust': '[[4.0, 0], [5.0, 100.0], [6.0, 200.0], [9.5, 300.0]]'}}, 'output': {'individual_curves': 'true'}}, 'srm_logic_tree': {'version': '', 'title': '', 'fault_systems': [{'short_name': 'HIK', 'long_name': 'Hikurangi-Kermadec', 'branches': [{'values': [{'name': 'dm', 'long_name': 'deformation model', 'value': 'TL'}, {'name': 'bN', 'long_name': 'bN pair', 'value': [1.097, 21.5]}, {'name': 'C', 'long_name': 'area-magnitude scaling', 'value': 4.0}, {'name': 's', 'long_name': 'moment rate scaling', 'value': 1.0}], 'sources': [{'nrml_id': 'SW52ZXJzaW9uU29sdXRpb25Ocm1sOjEyOTE2MDg=', 'rupture_rate_scaling': None, 'inversion_id': '', 'rupture_set_id': '', 'inversion_solution_type': '', 'type': 'inversion'}, {'nrml_id': 'RmlsZToxMzA3NDA=', 'rupture_rate_scaling': None, 'type': 'distributed'}], 'weight': 1.0, 'rupture_rate_scaling': 1.0}]}], 'logic_tree_version': 2}}
"""  # noqa
