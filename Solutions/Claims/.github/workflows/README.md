# GitHub Actions for this Repo

## BigQuery Deployment Workflow

<details>
    <summary><b>Overview</b></summary>
    
This workflow automates the deployment process for creating tables and views in BigQuery by making use of a [reusable workflow bigquery_workflow.yaml](https://github.com/HCACloudDataEngineering/gcp-hin-nonclinical-parent/tree/main/.github/workflows) that incorporates 2 custom composite actions. DDL scripts, UDFs, and Routines are stored only on GitHub, and the associated deploy python script ensures that components are built in the correct order to support associated dependencies (e.g. a table is built before a view that requires that table is built).
</details>

<details>
    <summary><b>How It Works</b></summary>   
    
The workflow is automatically triggered whenever a pull request that involves changes to the contents of the bigquery folder is merged into the dev, qa, or prod branches. The workflow can also be triggered mannually, but this should only be done in the event of a catastrophic failure that requires redeployment of all DDLs to GCP.     

This caller workflow passes several inputs to the reusable workflow:   
1. The repo_type, DDL or ETL (this has a bearing on how the environments config file(s) are handled)
2. The name of the branch that the PR will merge into
3. The type of event that triggered the workflow (automatically as part of merging a PR vs. manual deployment via workflow dispatch)
4. A GitHub Token value to allow the caller repo to make use of the contents of the parent repo
5. Workload Identity Federation and Service Account credentials to allow authentication to GCP

In addtion to the inputs defined within the caller workflow, expects the following repository variables to be available:
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
    
This caller workflow utilizes a [reusable workflow composer_workflow.yaml](https://github.com/HCACloudDataEngineering/gcp-hin-nonclinical-parent/tree/main/.github/workflows) that incorporates [2 custom composite actions](https://github.com/HCACloudDataEngineering/gcp-hin-nonclinical-parent/tree/main/composer) to make the correct environment config variables for Composer available and sync the designated folder within the caller repository to the DAGS folder in the GCS bucket associated with the relevant environment's proc project.

 </details>   

<details>
    <summary><b>How It Works</b></summary>
The workflow is automatically triggered when there is a merge into the dev, qa, or prod branches whenever the associated PR includes updates/additions to scripts within the dags folder. The workflow can also be triggered mannually.

This caller workflow passes several inputs to the reusable workflow:
1. The name of the branch that the PR will merge into
2. The file path to the config folder within the dags folder
3. An optional env_folder input to indicate the folder path where the environments config files are found; if left blank, this defaults to "environments"
4. The folder path to the subdomain folder within the dags folder to sync to the GCS bucket; if left blank, the workflow will sync the entire dags folder
5. Whether or not to enable 2-way syncing such that any items in the bucket that are NOT present in the GitHub repo are deleted from the bucket; if left blank, this feature is NOT enabled
6. An optional exclude_path input that allows for designation of any files/directories within the folder that will be synced to exclude from the sync; if left blank, no directories will be excluded from the path
7. Workload Identity Federation and Service Account credentials to allow authentication to GCP

In addtion to the inputs defined within the caller workflow, the reusable workflow checks the caller repo for the following environment repo variables:
1. PROC_PROJECT_ID
2. DAG_BUCKET_NAME
If either or both of these variables are not available as repo environment variables, the workflow will determine the relevant values from the variables within the environments config files.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow should be utilized to ensure that all DAG scripts that are pushed to an environment branch are synced into the DAGs folder within the relevant GCS storage bucket in order to be available to Composer.

</details>

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
