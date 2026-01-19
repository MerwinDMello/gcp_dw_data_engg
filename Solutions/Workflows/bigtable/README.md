# Bigtable Setup Composite Action

## Overview

This composite action prepares and processes configuration files for Google Cloud Bigtable resources in a GitHub Actions workflow. It detects changed or relevant Bigtable configuration files, extracts and merges environment details, injects project and instance information, and outputs ready-to-use artifacts for downstream deployment steps. The action supports DDL, ETL, and HIN2.0 repository structures and can be triggered by file changes or workflow dispatch.

## Steps

1. **Detect Relevant Bigtable Files**  
   - If triggered by workflow dispatch, collects all table configuration files under any `bigtable/*/tables/` directory, excluding markdown files, and copies them to the `artifacts` directory, preserving their structure.
   - If triggered by a code change, uses `tj-actions/changed-files` to detect changed files in the `bigtable` directory, and copies only those files to the `artifacts` directory.
   - If a changed config file is detected (e.g., `bigtable/example1/config/instance_config.yaml`), all files in the corresponding `tables` directory (e.g., `bigtable/example1/tables/`) are also copied to the `artifacts` directory.

2. **Prepare Environment Configuration**  
   - Extracts the `v_cur_project_id` variable from the appropriate environment config YAML for the branch and writes it to `env_config.yaml` in the workspace.
   - Ensures that the correct environment and project details are available for downstream steps.

3. **Extract Branch-Specific Config Sections**  
   - For each YAML or YML file in the `artifacts` directory, extracts only the section corresponding to the current branch (e.g., `dev`, `qa`, `prod`), and overwrites the file so that only that section remains.

4. **Add Environment Details to Configs**  
   - For each file in the `artifacts` directory, adds an `environment_details` section under `client_payload`, including the `project_id` from `env_config.yaml` and the `instance_id` from the associated `instance_config.yaml` for the current branch.
   - If the required `instance_config.yaml` is missing, the workflow fails with an error.

5. **Convert YAML to JSON**  
   - Converts all YAML and YML files in the `artifacts` directory to JSON format, preserving their paths and filenames (with a `.json` extension), and removes the original YAML/YML files so that only JSON files remain.

6. **Upload Artifacts**  
   - Uploads the processed `artifacts` directory as a GitHub Actions artifact for use in later workflow steps.

## Usage

Use this composite action at the start of your Bigtable deployment workflow to automatically detect changed or relevant configuration files, prepare their environment details, inject project and instance information, and output artifacts for downstream deployment steps. It supports both ETL and HIN2.0 repository structures and can be triggered by file changes or workflow dispatch.

**Inputs include:**
- `branch`: The branch/environment relevant to the workflow run (required).
- `domain`: The domain/subdomain of the caller repo, directs to the correct environments config file name (required).
- `env_folder`: The name of the folder containing environment YAMLs.
- `repo_type`: The caller repository type, either ETL or HIN2.0 (required).
- `token`: The access token (required).
- `dispatch`: Whether the workflow was triggered by a workflow dispatch event (optional, typically commented out).

# Bigtable Deploy Composite Action

## Overview

This composite action automates the deployment of Google Cloud Bigtable resources using prepared configuration and script artifacts in a GitHub Actions workflow. It sets up authentication, the Python environment, installs required dependencies, downloads artifacts, and executes the deployment script to create Bigtable resources. The action is designed to work seamlessly with the output from the Bigtable Setup Composite Action.

## Steps

1. **Authenticate to Google Cloud**  
   Uses the `google-github-actions/auth` action to authenticate with Google Cloud using the provided workload identity provider and service account.
2. **Download Artifacts**  
   Downloads the prepared Bigtable configuration files (`bigtable_artifacts`) and deployment scripts (`script_artifacts`) from previous workflow steps into the `artifacts` directory.
3. **List Artifact Contents**  
   Lists all files in the `artifacts` directory to verify the presence of configuration files and scripts.
4. **Set Up Python Environment**  
   Uses the `actions/setup-python` action to install Python 3.12 for running deployment scripts.
5. **Install Python Dependencies**  
   Installs required Python packages (`google-cloud-bigtable`, `google-cloud-core`, `pyyaml`, `schema`) using `pip` to ensure the deployment script has all necessary libraries.
6. **Deploy Bigtable Resources**  
   Executes the deployment script (`deploy.py`) using the configuration files in the `artifacts` directory. The script reads each config, connects to the appropriate Bigtable instance, and creates or updates tables and app profiles as specified.

## Usage

Use this composite action after the Bigtable Setup Composite Action in your workflow to deploy Bigtable resources based on the prepared configuration and scripts. It ensures the correct environment is set up and all dependencies are installed before running the deployment.

**Inputs include:**
- `workload_identity_provider`: The workload identity provider for Google Cloud authentication (required).
- `service_account`: The service account for Google Cloud authentication (required).

# Bigtable Deployment Script (`deploy.py`)

## Overview

This Python script automates the deployment of Google Cloud Bigtable resources using configuration files prepared by the setup workflow. It reads each configuration file, extracts environment details, and creates or updates Bigtable tables and app profiles as specified. The script is designed to be run as part of a GitHub Actions workflow and supports multiple environments and repository structures.

## Features

- **Schema Validation:** Uses the `schema` library to validate the structure of app profile and table configurations before processing.
- **Idempotent Resource Management:** Checks for the existence of app profiles and tables, updating them only if their configuration has changed.
- **Robust Error Handling:** Catches and logs errors for each file, app profile, table, and column family, and continues processing other resources.
- **Environment-Aware:** Extracts `project_id` and `instance_id` from each config file, ensuring resources are created in the correct environment.
- **Supports Both Single-Cluster and Multi-Cluster App Profiles:** Handles all relevant fields, including `multi_cluster_ids`, `routing_priority`, and normalizes values for accurate comparison.
- **Routing Priority Configuration:** Supports optional `routing_priority` parameter for app profiles to configure data ingestion and routing behavior.
- **Detailed Logging:** Prints informative messages for all major actions, updates, and errors.

## How It Works

1. **Parse Arguments**  
   Accepts a `--path` argument specifying the directory containing Bigtable configuration files to process.

2. **List and Process Configuration Files**  
   Iterates through all files in the specified directory, processing each as a Bigtable configuration.

3. **Read and Validate Configurations**  
   For each file:
   - Loads the JSON configuration.
   - Extracts `project_id` and `instance_id` from the `environment_details` section.
   - Validates the structure of app profiles and tables using the `schema` library.

4. **Create or Update Bigtable App Profiles**  
   For each app profile:
   - Checks if the app profile exists in the target instance.
   - If it does not exist, creates it with the specified routing policy, cluster IDs, routing priority (optional), and transactional write settings.
   - If it exists, fetches the current configuration and compares all relevant fields (`routing_policy_type`, `description`, `cluster_id`, `allow_transactional_writes`, `multi_cluster_ids`, `routing_priority`).  
     - **Normalization:** For `multi_cluster_ids`, treats `None` and `[]` as equivalent and sorts lists before comparison to avoid false positives.
   - Updates the app profile only if any field has changed.
   - **Routing Priority:** If specified, sets the routing priority for the app profile to control data ingestion and routing behavior.

5. **Create or Update Bigtable Tables and Column Families**  
   For each table:
   - Checks if the table exists in the target instance.
   - If it does not exist, creates it with the specified column families and garbage collection (GC) rules.
   - If it exists, iterates through the specified column families:
     - Updates the GC rule for existing column families if it has changed.
     - Creates new column families if they do not exist.

6. **Error Handling and Reporting**  
   - Logs errors for each file, app profile, table, or column family that fails to process.
   - Continues processing other resources even if some fail.
   - Exits with a non-zero code if any errors occurred, making it suitable for CI/CD pipelines.

## Usage

Use this script as part of your Bigtable deployment workflow to automate the creation and updating of Bigtable resources based on prepared configuration files. It is typically executed by a GitHub Actions step after the setup composite action.

**Arguments include:**
- `--path`: The path to the directory containing Bigtable configuration files (required).

**Requirements:**
- Python 3.12+
- Dependencies: `google-cloud-bigtable`, `google-cloud-core`, `pyyaml`, `schema`
- Valid configuration files with required environment details and resource specifications.