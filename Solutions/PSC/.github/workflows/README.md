# GitHub Actions for this Repo

## Move Adhoc & Hotfix to Archive Directory Workflow

<details>
    <summary><b>Overview</b></summary>
    
This workflow automates the process of moving adhoc hotfix files to an archive directory. It is designed to ensure that temporary or adhoc files are properly organized and archived on a regular schedule. The workflow leverages a [reusable workflow move_to_archive.yaml](https://github.com/HCACloudDataEngineering/gcp-hin-workflows/tree/main/.github/workflows) to perform the archiving process.
</details>

<details>
    <summary><b>How It Works</b></summary>   
    
The workflow can be triggered in two ways:
1. **Manually**: Using the `workflow_dispatch` event, allowing users to run the workflow on demand.
2. **Scheduled**: Using a cron schedule, the workflow runs daily at 1 PM UTC (8 AM CDT).

This caller workflow passes the necessary permissions and secrets to the reusable workflow:
1. **Permissions**:
   - `contents: write`: Allows the workflow to modify repository contents.
   - `id-token: write`: Enables authentication for accessing resources.
2. **Secrets**:
   - Inherits secrets from the repository to authenticate and perform the archiving process.

The reusable workflow handles the logic for identifying adhoc hotfix files and moving them to the designated archive directory.
</details>

<details>
    <summary><b>Usage</b></summary>

This workflow should be utilized to automate the archiving of adhoc hotfix files. It ensures that temporary files are regularly moved to an archive directory, reducing clutter and maintaining organization.

</details>

## BigQuery Deployment Workflow

<details>
    <summary><b>Overview</b></summary>
    
This workflow automates the deployment process for creating tables and views in BigQuery by making use of a [reusable workflow bigquery_workflow.yaml](https://github.com/HCACloudDataEngineering/gcp-hin-workflows/blob/main/.github/workflows/bigquery_workflow.yaml) that incorporates [2 custom composite actions and a python script](https://github.com/HCACloudDataEngineering/gcp-hin-workflows/tree/main/bigquery). DDL scripts, UDFs, and Routines are stored only on GitHub, and the associated deploy python script ensures that components are built in the correct order to support associated dependencies (e.g. a table is built before a view that requires that table is built).
</details>

<details>
    <summary><b>How It Works</b></summary>   
    
The workflow is automatically triggered whenever a pull request that involves changes to the contents of the bigquery folder is merged into the dev, qa, or prod branches. The workflow can also be triggered manually, but this should only be done in the event of a catastrophic failure that requires redeployment of all DDLs to GCP.     

This caller workflow passes several inputs to the reusable workflow:   
1. The repo_type, DDL or ETL (this has a bearing on how the environments config file(s) are handled)
2. The name of the branch that the PR will merge into
3. The type of event that triggered the workflow (automatically as part of merging a PR vs. manual deployment via workflow dispatch)
4. A GitHub Token value to allow the caller repo to make use of the contents of the parent repo
5. Workload Identity Federation and Service Account credentials to allow authentication to GCP

In addition to the inputs defined within the caller workflow, expects the following repository variables to be available:
1. For DDL repositories, the name of the data domain associated with the repository (this can also be done for ETL repos, but the value must be the overarching domain that is part of the project names associated with the repo)
2. Environment repo variables for the DEFAULT_DDL_PROJECT

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow should be utilized to automate the deployment process for tables and views in the correct environment on GCP.

</details>


## Composer Sync Workflow
<details>
    <summary><b>Overview</b></summary>
    
This caller workflow utilizes a [reusable workflow composer_workflow.yaml](https://github.com/HCACloudDataEngineering/gcp-hin-workflows/blob/main/.github/workflows/composer_workflow.yaml) that incorporates [2 custom composite actions]https://github.com/HCACloudDataEngineering/gcp-hin-workflows/tree/main/composer) to make the correct environment config variables for Composer available and sync the designated folder within the caller repository to the DAGS folder in the GCS bucket associated with the relevant environment's proc project.

 </details>   

<details>
    <summary><b>How It Works</b></summary>
The workflow is automatically triggered when there is a merge into the dev, qa, or prod branches whenever the associated PR includes updates/additions to scripts within the dags folder. The workflow can also be triggered manually.

This caller workflow passes several inputs to the reusable workflow:
1. The name of the branch that the PR will merge into
2. The file path to the config folder within the dags folder
3. An optional env_folder input to indicate the folder path where the environments config files are found; if left blank, this defaults to "environments"
4. The folder path to the subdomain folder within the dags folder to sync to the GCS bucket; if left blank, the workflow will sync the entire dags folder
5. Whether or not to enable 2-way syncing such that any items in the bucket that are NOT present in the GitHub repo are deleted from the bucket; if left blank, this feature is NOT enabled
6. An optional exclude_path input that allows for designation of any files/directories within the folder that will be synced to exclude from the sync; if left blank, no directories will be excluded from the path
7. Workload Identity Federation and Service Account credentials to allow authentication to GCP

In addition to the inputs defined within the caller workflow, the reusable workflow checks the caller repo for the following environment repo variables:
1. PROC_PROJECT_ID
2. DAG_BUCKET_NAME
If either or both of these variables are not available as repo environment variables, the workflow will determine the relevant values from the variables within the environments config files.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow should be utilized to ensure that all DAG scripts that are pushed to an environment branch are synced into the DAGs folder within the relevant GCS storage bucket in order to be available to Composer.

</details>

## Dataflow Setup Workflow

<details>
    <summary><b>Overview</b></summary>
    
This caller workflow utilizes a [reusable workflow dataflow_setup_workflow.yaml](https://github.com/HCACloudDataEngineering/gcp-hin-workflows/blob/main/.github/workflows/dataflow_setup_workflow.yaml) to orchestrate the setup phase for Google Cloud Dataflow jobs. It prepares the environment by identifying changed Dataflow job files, processing relevant configuration files, and generating a matrix of configuration paths for downstream build and deploy steps. The workflow supports both ETL and HIN2.0 repository structures.
</details>   

<details>
    <summary><b>How It Works</b></summary>

The workflow can be triggered manually or as part of a larger CI/CD process. It passes several inputs to the reusable workflow:
1. The name of the branch/environment relevant to the workflow run.
2. The path to the Dataflow directory in the repository.
3. The repository type, either ETL or HIN2.0.
4. An optional env_folder input to indicate the folder path where the environment config files are found (defaults to "environments" if not specified).

The reusable workflow performs the following:
- Checks out the repository to access all Dataflow job and configuration files.
- Detects which Dataflow files have changed.
- Processes and merges the relevant configuration files for each changed job.
- Prepares artifacts and a matrix of configuration paths for use in subsequent build and deploy jobs.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow should be utilized at the start of your Dataflow CI/CD process to automatically detect changed jobs, prepare their configuration, and output artifacts for downstream build and deployment steps.  
It supports both ETL and HIN2.0 repository structures and ensures that only relevant jobs are processed based on file changes.

**Inputs include:**
- `branch`: The branch/environment relevant to the workflow run (required).
- `dataflow_path`: The path to the Dataflow folder in the repository (default: `dataflow`).
- `repo_type`: The repository type, either `ETL` or `HIN2.0` (default: `HIN2.0`).
- `env_folder`: The folder containing environment YAML files (default: `environments`).

</details>

## Dataflow Build/Deploy Workflow

<details>
    <summary><b>Overview</b></summary>
    
This caller workflow utilizes a [reusable workflow dataflow_build_deploy_workflow.yaml](https://github.com/HCACloudDataEngineering/gcp-hin-workflows/blob/main/.github/workflows/dataflow_build_deploy_workflow.yaml) to automate the build and deployment of Google Cloud Dataflow jobs. It is triggered when a pull request is closed and merged into the `dev`, `qa`, or `prod` branches, specifically when changes are detected in the `data_engineering/dataflow` directory. The workflow ensures that any updated or new Dataflow jobs are built and deployed to the appropriate environment.
</details>   

<details>
    <summary><b>How It Works</b></summary>

The workflow is triggered automatically on PR close events targeting the main environment branches (`dev`, `qa`, `prod`) and when changes are made within the `data_engineering/dataflow` directory. It passes the following inputs to the reusable workflow:
1. The name of the base branch that the PR is being merged into (used to determine the target GCP environment).
2. The path to the Dataflow directory within the repository.

The workflow also passes a required secret (`gh_token`) for authentication to GitHub APIs and artifact downloads.

The reusable workflow then:
- Retrieves configuration artifacts from the setup workflow.
- Builds Dataflow Flex Templates for Python and Java jobs.
- Deploys streaming Dataflow jobs using the appropriate SDK (Python, Java, or YAML), leveraging matrix-driven parallelization for efficient processing.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow should be used to ensure that all new/updated Dataflow jobs in the `data_engineering/dataflow` directory are automatically built and deployed whenever a pull request is merged into an environment branch. It is ideal for maintaining up-to-date Dataflow pipelines across development, QA, and production environments.

**Inputs include:**
- `branch`: The base branch/environment relevant to the workflow run (automatically set to the PR's base branch).
- `dataflow_path`: The path to the Dataflow folder in the repository (set to `data_engineering/dataflow`).

**Secrets include:**
- `gh_token`: The GitHub token for authentication and artifact access (must be provided as a repository secret).

</details>

## GCS Workflows

Details can be found on the Data Transformation team [Production Support SharePoint page](https://hcahealthcare.sharepoint.com/sites/CORPDataTransformation/SitePages/Production-Support-Adhoc-Hotfix.aspx?csf=1&web=1&e=AXD1jI&CID=40a83ed1-ac0b-45aa-bd81-5c325934666f)

## ServiceCentral Ticket Automation script

<details>
    <summary><b>Overview</b></summary>
    
When a PR is merged into the prod branch, this script utilizes 2 reusable workflows to open a ServiceCentral change ticket and then mark it as complete. A note is added to the ServiceCentral ticket that includes a link to the associated PR.     
</details>   

<details>
    <summary><b>How It Works</b></summary>

1. When a PR for the prod branch has a status of closed, the script is initiated.
2. If the PR results in a merge, the script proceeds to initiate the reusable workflow from the HCACloudOps repo ru_change_management_open_prod.yml.
    - The identifier for the appropriate ServiceCentral ticket template (sys_id) is passed and a change ticket is created.
    - The change ticket number is captured.
3. The reusable workflow from the HCACloudOps repo ru_change_management_close_prod is then run.
    - The change ticket status is updated to "Complete."
    - A note is added to the Change Result field on the Closure Information tab that provides the link to the PR.
4. The change ticket status is confirmed to be "Complete."
</details>

<details>
    <summary><b>Usage</b></summary>

This script should be utilized to automatically create and complete a change ticket for all changes to the prod branch of the repo. Since the same change ticket template is used for all HIN prod branch changes and the link to the PR that is added to the close note is templated, this workflow can be run as-is in any HIN repo with no need to update with repo-specific variables.
</details>
