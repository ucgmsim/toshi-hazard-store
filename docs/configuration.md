# Configuration


The toshi_hazard_store project was originally designed to support the AWS Dynamodb database service. It now provides an option
to use a local sqlite3 store as an alternative.

 Caveats for local storage:

 - a complete model (e.g. the NSHM_v1.0.4 dataset) will likely prove too large for this option.
 - this is a single-user solution.
 - we provide no way to migrate data between storage backends (although in principle this should be relatively easy)


Run-time options let you configure the library for your use-case. Settings are made using environment variables, and/or a local `.env` (dotenv) file see [python-dotenv](https://github.com/theskumar/python-dotenv).

The '.env' file should be created in the folder from where the python interpreter is invoked - typically the root folder of your project.


### General settings

|         | Default | Description | for Cloud | for Local |
|---------|---------|-------------|-----------|-----------|
| **NZSHM22_HAZARD_STORE_STAGE**        | None | descriminator for table names | Required | Required |
| **NZSHM22_HAZARD_STORE_NUM_WORKERS**  | 1 | number of parallel workers for batch operations | Optional integer | NA (single worker only) |
| **THS_USE_SQLITE_ADAPTER**            | FALSE | use local (sqlite) storage? | NA | TRUE |


### Cloud settings

The NZSHM toshi-hazard-store database is available for public, read-only access using AWS API credentials (contact via email: nshm@gns.cri.nz).

  - AWS credentials will be provided with so-called `short-term credentials` in the form of an `awx_access_key_id` and and `aws_access_key_secret`.

  - Typically these are configured in your local credentials file as described in [Authenticate with short-term credentials](https://docs.aws.amazon.com/cli/v1/userguide/cli-authentication-short-term.html).

  - An `AWS_PROFILE` environment variable determines the credentials used at run-time by THS.


|         | Default | Description | for Cloud | for Local |
|---------|---------|-------------|-----------|-----------|
| **AWS_PROFILE**                       | None | Name of your AWS credentials | Required | N/A |
| **NZSHM22_HAZARD_STORE_REGION**       | None | AWS regaion e.g us-east-1 | Required | N/A |
| **NZSHM22_HAZARD_STORE_LOCAL_CACHE**  | None | folder for local cache  | Optional (leave unset to disable caching)| N/A |



### Local (off-cloud) settings

|         | Default | Description | for Cloud | for Local |
|---------|---------|-------------|-----------|-----------|
| **THS_SQLITE_FOLDER** | None | folder for local storage | N/A | Required


## Example .env file

```
# GENERAL settings
NZSHM22_HAZARD_STORE_STAGE=TEST
NZSHM22_HAZARD_STORE_NUM_WORKERS=4

# IMPORTANT !!
THS_USE_SQLITE_ADAPTER=TRUE

# CLOUD settings
AWS_PROFILE={YOUR AWS PROFILE}
NZSHM22_HAZARD_STORE_REGION={us-east-1)

# LOCAL Caching (Optional, cloud only)
NZSHM22_HAZARD_STORE_LOCAL_CACHE=/home/chrisbc/.cache/toshi_hazard_store

# LOCAL Storage settings
THS_SQLITE_FOLDER=/GNSDATA/LIB/toshi-hazard-store/LOCALSTORAGE
```

These settings can be overridden by specifiying values in the local environment.
