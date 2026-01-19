# BigQuery Composite Actions

## Setup Action
This action is designed to handle both manual and automatic triggers, manage artifacts, and ensure the correct environment configuration files are used based on the repository type.

## Inputs:

- `repo_type`: The type of repository (ETL or DDL) (required)
- `branch`: The relevant branch name (required)
- `domain`: The relevant domain (required)
- `token`: The access token (required)
- `dispatch`: Indicates if the workflow is triggered via workflow dispatch (default: "false")

## Steps:
### Get ALL Files:

- **Condition**: `inputs.dispatch == 'true'`
- **Shell**: `bash`
- **Commands**:
  - Echo the dispatch status
  - Create artifacts directory
  - Use `rsync` to copy files, excluding `adhoc` and `hotfix` folders
  - Alternative commented-out method using `find` and `cp`

### Find Changed Files:

- **Condition**: `inputs.dispatch != 'true'`
- **Uses**: `tj-actions/changed-files@v42`
- **With**: `files: 'bigquery/**/**'`

### Create Artifacts Directory:

- **Condition**: `inputs.dispatch != 'true'`
- **Shell**: `bash`
- **Commands**:
  - Create artifacts directory

### Place Changed Files in Artifacts Directory:

- **Condition**: `inputs.dispatch != 'true'`
- **Shell**: `bash`
- **Commands**:
  - Loop through changed files and copy them to artifacts directory

### Get the Environment Config File (ETL):

- **Condition**: `inputs.repo_type == 'ETL'`
- **Shell**: `sh`
- **Commands**:
  - Convert YAML to JSON and copy to artifacts directory

### Get the Environment Config File (DDL):

- **Condition**: `inputs.repo_type != 'ETL'`
- **Shell**: `sh`
- **Commands**:
  - Convert YAML to JSON and copy to artifacts directory

### Upload BigQuery Artifact:

- **Uses**: `actions/upload-artifact@v3`
- **With**:
  - `name: bigquery_artifacts`
  - `path: artifacts`
  - `retention-days: 1`

### Checkout Parent Repo:

- **Uses**: `actions/checkout@v2`
- **With**:
  - `repository: 'HCACloudDataEngineering/gcp-hin-nonclinical-parent'`
  - `token: ${{ inputs.token }}`

### List Scripts Folder:

- **Shell**: `bash`
- **Commands**:
  - List contents of `bigquery/scripts` directory

### Upload GitHub Artifact:

- **Uses**: `actions/upload-artifact@v3`
- **With**:
  - `name: script_artifacts`
  - `path: bigquery/scripts`
  - `retention-days: 1`

## Deploy Action
This action is designed to authenticate to Google Cloud, download necessary artifacts, list the artifacts, and deploy a BigQuery table using the specified inputs.

## Inputs:

- `branch`: The relevant branch name (required)
- `domain`: The relevant domain (required)
- `default_project_id`: The default project ID for the deployment of DDLs (required)
- `workload_identity_provider`: The workload identity provider for authentication (required)
- `service_account`: The service account for authentication (required)

## Steps:
### Authenticate to Google Cloud:

- **ID**: `auth`
- **Uses**: `google-github-actions/auth@v1.1.1`
- **With**:
  - `workload_identity_provider: ${{ inputs.workload_identity_provider }}`
  - `service_account: ${{ inputs.service_account }}`

### Download Artifacts:

- **Uses**: `actions/download-artifact@v3`
- **With**:
  - `name: bigquery_artifacts`
  - `path: artifacts`

### Download Script Artifacts:

- **Uses**: `actions/download-artifact@v3`
- **With**:
  - `name: script_artifacts`
  - `path: artifacts`

### Artifacts List:

- **Shell**: `bash`
- **Commands**:
  - List contents of `artifacts/bigquery` directory

### Deploy BQ Table:

- **Shell**: `sh`
- **Commands**:
  - Print working directory
  - List directory contents
  - Install `google-cloud-bigquery` package
  - Execute deployment script with specified inputs
- **Env**:
  - `project_id: ${{ inputs.default_project_id }}`

## Deployment Script
This script is designed to automate the deployment of BigQuery resources by reading SQL files, replacing placeholders with environment-specific values, and executing the queries using the BigQuery client. It also includes robust error handling to ensure any issues are logged and reported.

# Purpose:
The script is designed to deploy BigQuery resources (tables, views, procedures, and UDFs) based on SQL files and environment configurations.

# Key Components:

## Imports:
- **Standard libraries**: `json`, `os`, `argparse`, `traceback`, `re`
- **Google Cloud libraries**: `bigquery`, `NotFound`, `AlreadyExists`

## Global Variables:
- `bq`: BigQuery client instance
- `error_list`: List to store errors encountered during SQL execution

## Classes:
- `BigqueryDeployError`: Custom exception class for handling and printing SQL errors

## Functions:
- `read_config(path)`: Reads and returns JSON configuration from the specified path
- `read_sql(path)`: Reads and returns SQL content from the specified path
- `get_replacement_dictionary(config)`: Extracts and returns a dictionary of replacement parameters from the configuration
- `update_sql(sql, replacements)`: Replaces placeholders in the SQL with actual values from the replacements dictionary
- `run_query(sql, config)`: Executes the SQL query using the BigQuery client and handles errors
- `find_sql_files(root_folder)`: Recursively finds and returns all SQL files in the specified root folder
- `main()`: Entry point function that orchestrates the deployment process

# Main Workflow:

## Argument Parsing:
- Parses command-line arguments for `path`, `env`, and `domain`.

## Configuration Loading:
- Loads the environment configuration from a JSON file based on the provided domain.

## SQL File Discovery:
- Finds all SQL files in the specified path and categorizes them into tables, base views, views, procedures, and UDFs.

## SQL Execution:
- Executes SQL queries for each category (tables, base views, views, procedures, UDFs) and logs errors if any.

## Error Handling:
- Raises a custom `BigqueryDeployError` if any SQL errors are encountered during execution.

# Execution:
- The script is executed by calling the `main()` function when the script is run as the main module.
