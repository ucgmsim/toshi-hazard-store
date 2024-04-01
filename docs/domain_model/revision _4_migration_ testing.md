# Table migration testing

This describes performance of the v3 adnd v4 tables in sqlite and dynamodb


## Test outline

We used a Typical NSHM General Task R2VuZXJhbFRhc2s6MTMyODQxNA== which has VS30 = 275, large number of sites and all four tectonic regions as used in NSHM_V1.0.4

We test read and write performance in terms of time and AWS unit costs. For AWS differnent worker counts are tested.


## PynamoDB tests 

**April 1st 2024**

These are conducted on TryHarder (16 core workstatino) from Masterton NZ, connected to the **us-east-1** dynamodb service.

| Hazard calculation ID | HDF5 size | Revision / Table | Service |Object count | Workers | Time | Units/Sec avg | Unit Cost |
|--------------------------------------|--------|---------------------------------------------|---------|-------|----|-------|---|---|
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3 | 2.0 Gb | V3 / THS_OpenquakeRealization-TEST_CBC      | sqlite3 | 83811 | 1  | 2m50 | - | - |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3 | 2.0 Gb | V3 / THS_OpenquakeRealization-TEST_CBC | dynamodb | 83811 | 4 | 29m6 | 1800 | ? |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3 | 2.0 Gb | V4 / THS_R4_HazardRealizationCurve-TEST_CBC | dynamodb | 2262897 | 4 | 248m54 | 150 | ? |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3 | 2.0 Gb | V4 / THS_R4_HazardRealizationCurve-TEST_CBC | dynamodb | 2262897 | 4 | 248m54 | 150 | ? |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDE3 | 2.0 Gb | V4 / THS_R4_HazardRealizationCurve-TEST_CBC | dynamodb | 2262897 | 24 | 26m29 | 1900 | ? |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDI3 | 2.0 Gb | V3 / THS_OpenquakeRealization-TEST_CBC | dynamodb | 83811 | 8 | 15m4 | 3500 | ? |
| T3BlbnF1YWtlSGF6YXJkVGFzazoxMzI4NDMz | 2.0.Gb | V3 / THS_OpenquakeRealization-TEST_CBC | dynamodb | 83811 | 12 | 14m26 | 4500 | ? |