# HDF5 extraction: 'bad_file_3'
```
# first subtask of last gt in gt_index
# T3BlbnF1YWtlSGF6YXJkVGFzazo2OTI2MTg2 from R2VuZXJhbFRhc2s6NjkwMTk2Mw==
from hdf5_file = WORKING / 'openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazo2OTI2MTg2' / 'calc_1.hdf5' # bad file 3
```

##

CSV header
```
#,,,,,,,,,,,,,,,,,,"generated_by='OpenQuake engine 3.19.0', start_date='2024-03-22T00:44:16', checksum=3057760008, investigation_time=1.0, mag_bin_edges=[4.9975, 5.1974, 5.3972999999999995, 5.5972, 5.7970999999999995, 5.997, 6.196899999999999, 6.3968, 6.5967, 6.7966, 6.9965, 7.1964, 7.3963, 7.5962, 7.7961, 7.9959999999999996, 8.1959, 8.3958, 8.595699999999999, 8.7956], dist_bin_edges=[0.0, 5.0, 10.0, 15.0, 20.0, 30.0, 40.0, 50.0, 60.0, 80.0, 100.0, 140.0, 180.0, 220.0, 260.0, 320.0, 380.0, 500.0], lon_bin_edges=[164.00168479802613, 176.37431520197384], lat_bin_edges=[-47.8726, -38.8794], eps_bin_edges=[-4.0, -3.5, -3.0, -2.5, -2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0], tectonic_region_types=['Subduction Interface'], lon=170.188, lat=-43.376, weights=[0.07200000000000001, 0.09600000000000003, 0.10000000000000003, 0.07200000000000001, 0.07200000000000001, 0.10800000000000003, 0.08100000000000002, 0.07500000000000001, 0.07500000000000001, 0.09600000000000003, 0.08100000000000002, 0.07200000000000001], rlz_ids=[6, 10, 4, 9, 11, 1, 2, 3, 5, 7, 0, 8]"
imt,iml,poe,trt,mag,dist,eps,rlz6,rlz10,rlz4,rlz9,rlz11,rlz1,rlz2,rlz3,rlz5,rlz7,rlz0,rlz8
```



##  extractor

```
>>> WORKING = pathlib.Path('/GNSDATA/LIB/toshi-hazard-store/WORKING/DISAGG')
>>> # hdf5_file = WORKING / 'openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazoxMzU5MTQ1' / 'calc_1.hdf5' # bad file 4
>>> hdf5_file = WORKING / 'openquake_hdf5_archive-T3BlbnF1YWtlSGF6YXJkVGFzazo2OTI2MTg2' / 'calc_1.hdf5' # bad file 3
>>> OUTPUT_FOLDER = WORKING / "ARROW" / "DIRECT_DISAGG"
>>> dataset = ds.dataset(OUTPUT_FOLDER, format='parquet', partitioning='hive')
>>> table = dataset.to_table()
>>> df = table.to_pandas()
>>> df.tail()
       nloc_001                   trt magnitude distance epsilon      imt   rlz  vs30           poe nloc_0
140347      MRO  Subduction Interface        18       16      15  SA(3.0)  rlz3   750  2.382023e-09    MRO
140348      MRO  Subduction Interface        18       16      15  SA(3.0)  rlz5   750  2.382023e-09    MRO
140349      MRO  Subduction Interface        18       16      15  SA(3.0)  rlz7   750  2.382023e-09    MRO
140350      MRO  Subduction Interface        18       16      15  SA(3.0)  rlz0   750  2.382023e-09    MRO
140351      MRO  Subduction Interface        18       16      15  SA(3.0)  rlz8   750  2.382023e-09    MRO
>>>
```

### oqparam

```
>>> oqparam = json.loads(extractor.get('oqparam').json)
>>> oqparam
{'base_path': '/WORKING/config_1', 'inputs': {'job_ini': '/WORKING/config_1/job.ini', 'source_model_logic_tree': '/WORKING/config_1/sources/sources.xml',
	'site_model': ['/WORKING/config_1/sites.csv'], 'gsim_logic_tree': '/WORKING/config_1/gsim_model.xml'},

'description': 'Disaggregation for site: -43.376~170.188, vs30: 750, IMT: SA(3.0), level: 0.006488135117', 'random_seed': 25,
'calculation_mode': 'disaggregation', 'ps_grid_spacing': 30.0, 'reference_vs30_value': 750.0, 'reference_depth_to_1pt0km_per_sec': 44.0, 'reference_depth_to_2pt5km_per_sec': 0.6, 'reference_vs30_type': 'measured', 'investigation_time': 1.0, 'truncation_level': 4.0,
'maximum_distance': {'Active Shallow Crust': [[4.0, 0], [5.0, 100.0], [6.0, 200.0], [9.5, 300.0]], 'Subduction Interface': [[5.0, 0], [6.0, 200.0], [10, 500.0]], 	'Subduction Intraslab': [[5.0, 0], [6.0, 200.0], [10, 500.0]], 'default': [[5.0, 0], [6.0, 200.0], [10, 500.0]]},
	'iml_disagg': {'SA(3.0)': [0.006488135116816442]}, 'max_sites_disagg': 1,
	'mag_bin_width': 0.1999, 'distance_bin_width': 10.0, 'coordinate_bin_width': 5.0, 'num_epsilon_bins': 16,
	'disagg_outputs': ['TRT', 'Mag', 'Dist', 'Mag_Dist', 'TRT_Mag_Dist_Eps'],
	'disagg_bin_edges': {'dist': [0, 5, 10, 15, 20, 30,40, 50, 60, 80, 100, 140, 180, 220, 260, 320, 380, 500]},
	'number_of_logic_tree_samples': 0, 'rupture_mesh_spacing': 4.0, 'width_of_mfd_bin': 0.1, 'complex_fault_mesh_spacing': 10.0,
	'area_source_discretization': 10.0, 'exports': [''], 'individual_rlzs': 1, 'hazard_imtls': {'SA(3.0)': [0.006488135116816442]},
	'pointsource_distance': {'default': 40.0}, 'all_cost_types': [], 'minimum_asset_loss': {}, 'collect_rlzs': 0, 'export_dir': '/WORKING/config_1'}
>>>
```

#### extractor meta
```
>>> disagg_rlzs = extractor.get(
...     f'disagg?kind=TRT_Mag_Dist_Eps&imt=SA(3.0)&site_id=0&poe_id=0&spec=rlzs' , asdict=True)

>>> disagg_rlzs.keys()
dict_keys(['kind', 'imt', 'site_id', 'poe_id', 'spec', 'trt', 'mag', 'dist', 'eps', 'poe', 'traditional', 'shape_descr', 'weights', 'extra', 'array'])
>>> disagg_rlzs['trt']
array([b'Subduction Interface'], dtype='|S20')
```

```
>>> disagg_rlzs = extractor.get(
...     f'disagg?kind=TRT_Mag_Dist_Eps&imt=SA(3.0)&site_id=0&poe_id=0&spec=rlzs' , asdict=False)
>>> disagg_rlzs.trt
array([b'Subduction Interface'], dtype='|S20')
>>> disagg_rlzs.eps
array([-3.75, -3.25, -2.75, -2.25, -1.75, -1.25, -0.75, -0.25,  0.25,
        0.75,  1.25,  1.75,  2.25,  2.75,  3.25,  3.75])
>>> disagg_rlzs.mag
array([5.09745, 5.29735, 5.49725, 5.69715, 5.89705, 6.09695, 6.29685,
       6.49675, 6.69665, 6.89655, 7.09645, 7.29635, 7.49625, 7.69615,
       7.89605, 8.09595, 8.29585, 8.49575, 8.69565])
>>>
```

### RLZ_LT
```
rlz
   branch_path  weight                    source combination                        Subduction Interface
0          A~A   0.081  [dm0.7, bN[0.902, 4.6], C4.0, s0.28]                    Atkinson2022SInter_Upper
1          A~B   0.108  [dm0.7, bN[0.902, 4.6], C4.0, s0.28]                  Atkinson2022SInter_Central
2          A~C   0.081  [dm0.7, bN[0.902, 4.6], C4.0, s0.28]                    Atkinson2022SInter_Lower
3          A~D   0.075  [dm0.7, bN[0.902, 4.6], C4.0, s0.28]  NZNSHM2022_AbrahamsonGulerce2020SInter_GLO
4          A~E   0.100  [dm0.7, bN[0.902, 4.6], C4.0, s0.28]  NZNSHM2022_AbrahamsonGulerce2020SInter_GLO
5          A~F   0.075  [dm0.7, bN[0.902, 4.6], C4.0, s0.28]  NZNSHM2022_AbrahamsonGulerce2020SInter_GLO
6          A~G   0.072  [dm0.7, bN[0.902, 4.6], C4.0, s0.28]        NZNSHM2022_ParkerEtAl2020SInter_true
7          A~H   0.096  [dm0.7, bN[0.902, 4.6], C4.0, s0.28]        NZNSHM2022_ParkerEtAl2020SInter_true
8          A~I   0.072  [dm0.7, bN[0.902, 4.6], C4.0, s0.28]        NZNSHM2022_ParkerEtAl2020SInter_true
9          A~J   0.072  [dm0.7, bN[0.902, 4.6], C4.0, s0.28]          NZNSHM2022_KuehnEtAl2020SInter_GLO
10         A~K   0.096  [dm0.7, bN[0.902, 4.6], C4.0, s0.28]          NZNSHM2022_KuehnEtAl2020SInter_GLO
11         A~L   0.072  [dm0.7, bN[0.902, 4.6], C4.0, s0.28]          NZNSHM2022_KuehnEtAl2020SInter_GLO
```


### SRC_LT
```
src
                                           branch branchset        utype                                             uvalue  weight
branch_code
A            [dm0.7, bN[0.902, 4.6], C4.0, s0.28]       PUY  sourceModel  'SLT_v9p0p0/PUY/[dm0.7,bN[0.902,4.6],C4.0,s0.2...     1.0
```

### GSM_LT
```
>>> gsm
                     trt branch                                        uncertainty  weight
0   Subduction Interface    gA1  [Atkinson2022SInter]\nepistemic = "Upper"\nmod...   0.081
1   Subduction Interface    gB1  [Atkinson2022SInter]\nepistemic = "Central"\nm...   0.108
2   Subduction Interface    gC1  [Atkinson2022SInter]\nepistemic = "Lower"\nmod...   0.081
3   Subduction Interface    gD1  [NZNSHM2022_AbrahamsonGulerce2020SInter]\nregi...   0.075
4   Subduction Interface    gE1  [NZNSHM2022_AbrahamsonGulerce2020SInter]\nregi...   0.100
5   Subduction Interface    gF1  [NZNSHM2022_AbrahamsonGulerce2020SInter]\nregi...   0.075
6   Subduction Interface    gG1  [NZNSHM2022_ParkerEtAl2020SInter]\nsigma_mu_ep...   0.072
7   Subduction Interface    gH1  [NZNSHM2022_ParkerEtAl2020SInter]\nsigma_mu_ep...   0.096
8   Subduction Interface    gI1  [NZNSHM2022_ParkerEtAl2020SInter]\nsigma_mu_ep...   0.072
9   Subduction Interface    gJ1  [NZNSHM2022_KuehnEtAl2020SInter]\nregion = "GL...   0.072
10  Subduction Interface    gK1  [NZNSHM2022_KuehnEtAl2020SInter]\nregion = "GL...   0.096
11  Subduction Interface    gL1  [NZNSHM2022_KuehnEtAl2020SInter]\nregion = "GL...   0.072
>>>
```
