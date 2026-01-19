# Initial Repo Setup 

(delete this section once setup is completed)

1. Under Settings > (Access section)Teams confirm that all teams that should have access have been added, adjust as needed
2. Review the branch protection rule that is in place for the prd branch; ensure it aligns with your desired workflow (Settings > (Code and automation section)Branches then click on the rule name)
3. Add the following variables to the [Terraform workspace](https://app.terraform.io/app/hca-healthcare/workspaces/hca-hin-monitoring-parallon-eim-prod):   
   - logging_bucket (the hca-hin-monitoring-parallon bucket)
    - monitoring_project_id (for Parallon, this is hca-hin-monitoring-parallon)
    - pagerduty_api_key (to allow Pager Duty account integration)
4. Review the READMEs within the following folders, complete setup as they describe:
    - workflows
    - terraform
    - docs

(this is the end of the section to delete once setup is complete)

<!-- ABOUT THE PROJECT -->
## **About The Project**
---

This repo holds the code for deploying alerts, log based metrics, dashboards etc. to hca-hin-monitoring-\<DOMAIN> project on GCP. GitHub Actions are in place to run the terraform plan command on creation of a pull request and then run the terraform apply command on a merge into the prd branch.

## **Requirements**

* Terraform Workspace (Ryan Cook's team sets up workspace and connects to your GitHub repo)
* GCP Scoping Project
* Service account in scoping project with monitoring and logging admin role

## **Terraform Workspace Variables**   

These variables must be set up in the associated Terraform Workspace using these exact names

* **gcp_region**
  * Region associated with the GCP monitoring project
* **logging_bucket**
  * GCS logging sink bucket
* **monitoring_project_id**
  * Set to the project id of the gcp monitoring project being deployed to
* **pagerduty_api_key (optional)**
  * Used for pagerduty service integration

## **How to Add New Alerts**
---

1. Create new feature branch from prd
2. Inside the feature branch, create or update the alert in the cloud_composer_alerts.tf script within the terraform folder.
3. Push branch to repo and create a pull request to merge into prd
4. Once all checks pass in pull request, confirm merge into prd and delete feature branch

Once the above steps are completed, GitHub Actions will run a terraform apply to add the changes. You will see the updates in the GCP project once completed.

## **Resources and References**
---

[HCA GCP Monitoring - Alerting How To Guide](https://confluence.app.medcity.net/c/display/PCS/GCP+Cloud+Monitoring+-+Alerting+How+To+Guide)   
[HCA GCP Monitor Scoping Module](https://app.terraform.io/app/hca-healthcare/registry/modules/private/hca-healthcare/monitor-scoping/gcp/2.0.3#modules)   
[HCA GCP Alert Module](https://app.terraform.io/app/hca-healthcare/registry/modules/private/hca-healthcare/alerting/gcp/0.0.9)   
[HCA GCP Standard Alert Module](https://app.terraform.io/app/hca-healthcare/registry/modules/private/hca-healthcare/standard-alerting/gcp/0.0.5#how-to-use-this-module)   
[Terraform Documentation - Google Monitoring Alert Policy](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/monitoring_alert_policy#nested_condition_threshold)   
