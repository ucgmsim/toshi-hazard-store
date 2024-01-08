
Users may choose to store data locally instead of the default AWS DynamoDB store. Caveats:

 - The complete NSHM_v1.0.4 dataset will likely prove too large for this option.
 - this is single-user only
 - currently we provide no way to migrate data between storage backends (although in principle this should be relatively easy)


## Environment configuration

```
SQLITE_ADAPTER_FOLDER = os.getenv('THS_SQLITE_FOLDER', './LOCALSTORAGE')
USE_SQLITE_ADAPTER = boolean_env('THS_USE_SQLITE_ADAPTER')
```
## CLI for testing

We pro
'