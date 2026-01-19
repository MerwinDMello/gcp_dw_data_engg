# Data Quality DAG Setup Composite Action

## Overview

This composite action detects changed Data Quality (DQ) use case folders and builds a matrix of use cases to process for downstream DAG generation and deployment. It supports both incremental deployment (based on changed files) and full redeployment (via workflow dispatch). The action outputs a matrix of use cases for use in subsequent workflow steps.

## Steps

1. **Detect Changed Use case Folders**
   - If triggered by workflow dispatch, collects all use case folders in the `data_quality` directory for redeployment.
   - If triggered by a code change, uses `git diff` to detect changed files within `data_quality/usecase_*/` folders (excluding markdown files).
2. **Process Changed Files**
   - Deduplicates and extracts the relevant use case folders from the changed or collected files.
3. **Build Matrix Output**
   - Constructs a JSON matrix of use cases to process, which is output for downstream steps.
   - If no use cases are found, outputs an empty matrix and exits early.
4. **Log Matrix Contents**
   - Prints the matrix contents to the GitHub Actions log for visibility and debugging.

## Usage

Use this composite action at the start of your Data Quality DAG deployment workflow to automatically detect changed or relevant usecases and output a matrix for downstream DAG generation and deployment steps.

**Inputs include:**
- `dispatch`: Whether the workflow was triggered by a workflow dispatch event (required).
- `previous_sha`: The previous commit SHA before the push (required).
- `current_sha`: The current commit SHA after the push (required).

**Outputs include:**
- `matrix`: Matrix of processed usecases for downstream DAG generation and deployment steps.

---

# Data Quality DAG Deploy Composite Action

## Overview

This composite action automates the generation and deployment of Data Quality DAGs and associated spec files to Google Cloud Storage (GCS). It handles environment validation, DAG generation from templates and configs, authentication, and file uploads. The action supports both create/update and delete DAGs for each use case.

## Steps

1. **Set Up Python Environment**
   - Installs Python and required dependencies (`pyyaml`, `yq`).
2. **Validate Environment**
   - Checks that the specified environment exists in the usecase’s config before proceeding.
3. **Generate DAGs**
   - Uses a Python script to generate DAG files from templates and merged config values for the specified environment and usecase.
4. **Prepare Upload Directories**
   - Copies generated DAG scripts and config files to a composer upload directory.
   - Copies spec files to a spec upload directory, logging warnings if expected files are missing.
5. **Authenticate to Google Cloud**
   - Uses Workload Identity Federation and a specified service account for authentication.
6. **Resolve Composer and Spec Buckets**
   - Extracts bucket paths from config files, with error handling and logging for missing values.
7. **Upload Files to GCS**
   - Uploads DAG scripts and spec files to the appropriate GCS buckets and paths for Airflow and Dataplex consumption.

## Usage

Use this composite action in your workflow to automate the generation and deployment of Data Quality DAGs and spec files for each use case. The action will handle environment validation, DAG generation, authentication, and file uploads, ensuring your DAGs are deployed to the correct environment and GCS location.

**Inputs include:**
- `usecase`: The use case relevant for DAG generation (required).
- `branch`: The branch/environment being deployed (required).
- `domain_config_path`: Path to the domain configuration file (required).
- `dag_create_update_script_path`: Path to the DAG create/update template script (required).
- `dag_delete_script_path`: Path to the DAG delete template script (required).
- `workload_identity_provider`: Workload identity provider for Google Cloud authentication (required).
- `service_account`: Service account for Google Cloud authentication (required).

**Outputs:**
- No explicit outputs; uploads files to GCS for downstream Airflow and Dataplex workflows.