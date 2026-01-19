# Initial Set Up Instructions for the workflows folder

(Delete this section once completed)

## Terraform HIN workflow (tf-hin-workflow.yml) setup
1. Requirements: 
    - TF_API_TOKEN added as a repo secret
    - Variables set up/defined in the terraform folder scripts
2. No adjustments need to be made to the script to tailor it to a given domain

## Terraform Scoping workflow (tf-scoping-workflow.yml) setup
1. Requirements:
    - Variables set up/defined in the terraform folder scripts
2. Parallon's Terraform workspace name has already been put in place

## Service Central workflow (servicecentral.yml) setup
1. Requirements: None
2. If you wish to use a different ServiceCentral template than the original that was established, you will need to have that created and update the sys_id in the "open" job variable accordingly
3. If there is different/additional information you would like captured in the Change Result field, adjust the close_notes variable in the "complete" job   

(end of section to delete once setup is complete)

# GitHub Actions for this Repo


## Terraform HIN Workflow
<details>
    <summary><b>Overview</b></summary>   

This workflow is a basic Terraform reusable workflow. It defines the inputs, sets default values for those inputs, sets environment variables, and includes 4 steps in the terraform-workflow job. These steps involve checking out the GitHub repository, a TFLint step which is always bypassed in the context of this repository, initializing Terraform and passing it CLI commands, and uploading the output plan file. This workflow is not run directly, but instead is used by the TF Scoping Workflow script.
 </details>   

<details>
    <summary><b>How It Works</b></summary>
   
 1. This workflow is activated when it is called by another workflow call, in this case the TF Scoping Workflow.
 2. Several inputs are named and configured, including default values.
 3. One secret, TF_API_TOKEN, is named and defined as required.
 4. There is one job in this workflow, named Terraform Workflow.
 5. The environment name, relevant environment variables, a default shell, and a default working directory are defined.
 6. The first step of the workflow job is to checkout the GitHub repository.
 7. The next step of the job is named TFLint - this step is always bypassed when the workflow runs in this repository.
 8. The third step, named Terraform CLI, initializes Terraform and passes commands to it. It is in this step, based on the values passed to the tf-plan and tf-apply variables, where terraform plan and/or terraform apply are run.
    - The terraform plan command creates an execution plan that Terraform will make to the GCP infrastructure for monitoring and alerting in this domain; additional details are available here:   
    https://developer.hashicorp.com/terraform/cli/commands/plan
    - The terraform apply command executes the actions from the Terraform plan; additional details are available here:   
    https://developer.hashicorp.com/terraform/cli/commands/apply
9. The final step only runs if the tf-plan variable is set to true, and it serves to make the Terraform plan file available to the next run of the workflow, when the tf-apply variable is set to true. This will be discussed further in the Terraform Scoping Workflow section.

</details>

<details>
    <summary><b>Usage</b></summary>
This workflow is called by the Terraform Scoping Workflow, and exact conditions under which that workflow is activated will be discussed in the next section. The Terraform HIN Workflow is what initializes Terraform and makes the contents of this repository and the associated Terraform workspace available to it so that the logging metrics and alerts are set up within GCP.

</details>

## Terraform Scoping Workflow 
<details>
    <summary><b>Overview</b></summary>
    
This workflow includes two jobs: TF SCOPING PLAN that runs whenever a Pull Request is created and TF SCOPING APPLY that runs whenever there is a merge into the prd branch. Both jobs activate the Terraform HIN Workflow and the steps described within it, but in the case of the PLAN job, the tf-plan variable is set to true and the tf-apply variable is set to false, and in the case of the APPLY job, the these values are reversed. In this way, the terraform plan command is executed with the creation of a Pull Request into the prd branch, making the Terraform plan file available for the terraform apply command to use when it is run upon merging of the Pull Request into the prd branch.

</details>   

<details>
    <summary><b>How It Works</b></summary>

1. The first job in this workflow is called TF SCOPING PLAN
    - This job runs when a Pull Request is created OR when triggered manually
    - This job calls the Terraform HIN Workflow, setting the tf-plan variable to true and the tf-apply variable to false (see the details of how this impacts the steps of the Terraform HIN Workflow in the section above)
2. The second job in this workflow is called TF SCOPING APPLY
    - This job runs when a Pull Request into the prd branch is closed and merged
    - This job calls the Terraform HIN Workflow, setting the tf-plan variable to false and the tf-apply variable to true (see the details of how this impacts the steps of the Terraform HIN Workflow in the section above)   

</details>

<details>
    <summary><b>Usage</b></summary>
This workflow is used to call the Terraform HIN Workflow, first to create and upload the Terraform plan file, then to execute that plan. When reviewing Action runs for this repository, it is under Terraform Scoping Deployment that you will find all the activity as this is the workflow that is responsible for running the actions.   

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
    
