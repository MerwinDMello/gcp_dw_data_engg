# GitHub Actions for the gcp-hin-hr Repo

## ServiceCentral Ticket Automation script

<details>
    <summary><b>Overview</b></summary>
    
This script utilizes 2 reusable workflows to open a ServiceCentral change ticket and then mark it as complete when a PR is merged into the prod branch. A note is added to the ServiceCentral ticket that includes a link to the associated PR.     
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

This script should be utilized to automatically create and complete a change ticket for all changes to the prod branch of the repo. Since the same change ticket template is used for all HIN prod branch changes and the link to the PR that is added to the close note is templated, this workflow can be run as is in any HIN repo with no need to update with repo-specific variables.
</details>
    