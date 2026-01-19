# Initial Set Up Instructions for the workflows folder
(Delete this section once completed)

## Composer workflow setup
1. Requirements:
    - Folders/Files:
        - Environment configuration files in the "environments" folder
        - v_proc_project_id
        - v_dag_bucket_name

## GSM workflow setup
### You are unlikely to need to utilize this workflow in this repository. Secrets deployed to GSM should be handled through the general domain repository for this subdomain

1. Requirements:
    - Folders/Files:
        - Environment configuration files in the "environments" folder
        - A "terraform" folder containing relevant terraform scripts defining variables, etc. You will need to create these variables in the scripts.
    - Environment variables defined within the above files:
        - v_proc_project_id
        - v_dag_bucket_name
    - Any additional secrets that need to be created in GSM
2. Define the secrets in the final "env" section of the workflow script:
    - If the relevant secret is different based on the environment involved (e.g. dev vs. prd), then you will need to make use of GitHub environments

## Service Central workflow setup
1. Requirements: None
2. If you wish to use a different ServiceCentral template than the original that was established, you will need to have that created and update the sys_id in the "open" job variable accordingly
3. If there is different/additional information you would like captured in the Change Result field, adjust the close_notes variable in the "complete" job
(end of section to delete once setup is complete)

# GitHub Actions for this Repo


## Composer script
<details>
    <summary><b>Overview</b></summary>
This script utilizes several actions from [google-github-actions](https://github.com/google-github-actions) to set up environment files and variables for Composer and sync the DAGs folder with the repository's GCS bucket associated with the relevant environment (dev, qa, prd) 

 </details>   

<details>
    <summary><b>How It Works</b></summary>
   
 1. The worflow is initiated when there is a push to any of the environment branches (e.g. dev, qa, prd), and is set up such that that variables will be imported based on the specific environment involved.
 2. The workflow first checks out the repository, sets up the environment files, imports variables based on the environment, and saves the configuration file(s) to the appropriate subfolder within the dags folder.
 3. Next, Bash commands are used to convert the YAML file to JSON format.
 4. Using a GitHub action (rgarcia-phi/json-to-variables@v1.1.0), the json file is then converted to variables.
 5. In the next step, Google Cloud authentication is done using google-github-actions/auth@v1 utilizing Workload Identity Federation. This allows subsequent steps to inherit the authentication token.
 6. GCP setup is then completed using another GitHub action.
 7. Finally, another GitHub action is used to sync the DAGs folder with the appropriate GCS bucket.
</details>

<details>
    <summary><b>Usage</b></summary>

This workflow should be utilized to ensure that all DAGs that are pushed to an environment branch are synced into the DAGs folder within the relevant GCS storage bucket in order to be available to Composer.

</details>

## Google Secret Manager (GSM) script

### This workflow is not likely to be utilized in this repository as deployment of secrets to GSM will be handled by the general domain repository for this subdomain.

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
    
