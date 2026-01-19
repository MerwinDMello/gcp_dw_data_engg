# GitHub Actions for this Repo

## Composer script
<details>
    <summary><b>Overview</b></summary>
The Composer DAG Sync workflow is designed to automate the deployment of Directed Acyclic Graphs (DAGs) to Google Cloud Composer environments upon pushes to dev, qa, and prd branches, or through manual triggers. It utilizes a reusable workflow to sync DAG files and configurations from specific paths within this GitHub repository to Google Cloud, with support for environment-specific authentication via dynamically configured secrets. The workflow is configured to not perform two-way synchronization by default, with detailed instructions provided for enabling this feature if necessary.

 </details>   

<details>
    <summary><b>How It Works</b></summary>

### Call Composer Reusable Workflow

This job calls a reusable workflow located at [`HCACloudDataEngineering/gcp-hin-nonclinical-parent/.github/workflows/composer_workflow.yaml@main`](https://github.com/HCACloudDataEngineering/gcp-hin-nonclinical-parent/blob/main/.github/workflows/composer_workflow.yaml). It passes several parameters to the reusable workflow, configuring it for the specific deployment scenario.

#### Inputs

- `branch`: The name of the branch that triggered the workflow. This is dynamically set to `${{ github.ref_name }}`.
- `config_filepath`: Specifies the path to the configuration file within the DAGs directory. Set to `dags/config/edwpi`.
- `dags_subdomain_folder_path`: An empty string is provided, indicating that the entire `dags` folder should be synced.
- `two_way_sync`: An empty string is provided, indicating that 2-way sync (deletion of unmatched destination objects) is not enabled. Instructions are provided for enabling this feature in the future.
- `exclude_path`: An optional variable to add one or more paths within the designate folder to exclude from the sync

#### Secrets

- `workload_identity_provider`: Configured dynamically based on the branch name, allowing for different identity providers per environment.
- `service_account`: Also configured dynamically based on the branch name, specifying the service account to use for authentication with Google Cloud Platform.

#### Notes

 - The worflow is initiated when there is a push to any of the environement branches (e.g. dev, qa, prd), and is set up such that that variables will be imported based on the specific environment involved.
 - This workflow will only run if there are changes in one or more of the following: files in the `environments` folder, files in the `dags` folder, the workflow script itself.
 - The underlying reusable workflow leverages 2 custom-built composite actions to accomplish [Build](https://github.com/HCACloudDataEngineering/gcp-hin-nonclinical-parent/blob/main/composer/build/action.yaml) and [Deploy](https://github.com/HCACloudDataEngineering/gcp-hin-nonclinical-parent/blob/main/composer/deploy/action.yaml) steps.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow should be utilized to ensure that all DAGs that are pushed to an environment branch are synced into the DAGs folder within the relevant GCS storage bucket in order to be available to Composer. To use this workflow in your reop, ensure that your repository structure includes `environments` and `dags` directories as specified in the trigger paths. Configure the required secrets in your GitHub repository settings to match the expected format for `workload_identity_provider` and `service_account`.

</details>

## Google Secret Manager (GSM) script

### This workflow is not likely to be utilized in this repository as secrets are handled by the [HCA-PDM/gcp-hin-parallon](https://github.com/HCA-PDM/gcp-hin-parallon) repo

<details>
    <summary><b>Overview</b></summary>
    
This script uses Terraform to create secrets in Google Secret Manager from secrets stored in GitHub.
</details>   

<details>
    <summary><b>How It Works</b></summary>

1. The worflow is initiated when there is a push to any of the environement branches (e.g. dev, qa, prd), and is set up such that variables will be imported based on the specific environment involved.
2. The workflow first accomplishes three things:
    - checkout the repository
    - setup the environment files
    - import variables based on the environment
3. Next, Bash commands are used to convert the YAML file to JSON format.
4. Using a GitHub action (rgarcia-phi/json-to-variables@v1.1.0), the json file is then converted to variables.
5. In the next step, Google Cloud authentication is done using google-github-actions/auth@v1 utilizing Workload Identity Federation. This allows subsequent steps to inherit the authentication token.
6. A different GitHub action is then used for Terraform setup, making variables in the repo available to Terraform.
7. The final step executes Terraform code to assign the secrets and variables values to their corresponding Terraform variables so that Terraform can then apply these changes, creating the relevant secrets in GSM for the associated project.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow is utilized to create any secrets required to exist in Google Secret Manager. In order for the script to create the secrets in GSM, supporting information must be added in the following places:
1. GitHub Secrets for this repo
2. In .tf files within the "terraform" folder
3. Defined as variables in the "env" section of the workflow script
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
    
