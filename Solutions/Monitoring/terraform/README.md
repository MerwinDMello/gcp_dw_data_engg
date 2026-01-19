# Initial Set Up Instructions for the terraform folder
(Delete this section once completed)

## Logging Metrics script
1. Requirements:
    - Insert the domain name using lowercase letters in all values as indicated by \<domain> (e.g. for the Parallon domain, all instances of \<domain> would be replaced by parallon)
        - Look in the top line of each resource definition, the name line, and the logName line to find all the places that need the domain inserted
2. The script is set up to provide metrics for the start and end time of production load cycles as well as composer job failures for the qa and prod projects. If you wish to have additional metrics, you would need to create them here

## Cloud Composer Alerts script
This is the script that will require the most configuration specific to each domain

**IMPORTANT: Do NOT make initial changes to both the Cloud Composer Alerts script and the Logging Metrics script in the same PR. Doing so will result in errors. Complete initial setup in the Logging Metrics script and complete a PR, then do a subsequent PR for the Clour Composer Alerts**

1. Requirements: 
    - For the top two alerts, insert the domain name using lowercase letters in all values as indicated by \<domain> (e.g. for the HR domain, all instances of \<domain> would be replaced by hr)
    - For each load cycle that you wish to receive start and end time alerts, you will need to create an alert entry. A few examples are provided (commented out) for your reference. Delete these when no longer needed
        - There are 2 ways to identify the start of a load cycle
            - The start of the first job of the ingest DAG that starts the cycle
            - The end of a sensor job that precedes the start of the cycle
        - The end of a load cycle is identified by the end of the last job of the final integrate DAG of the cycle
        - These are the areas that will need to be tailored to each start or end alert:
            - Alert name in the top resource line
            - display_name
            - content name within documentation section
            - display_name within conditions section
            - filter
                - Which google_logging_metric to use (start or end, align with metric names from the Logging metrics script)
                - The two has_substring filters
                    - The first substring is the name of the DAG that should be referenced for the start or end of the cycle
                    - The second substring is the name of the job within the DAG that should be referenced for the start or end of the cycle

## Main script
1. No setup needed, leave blank

## Notification Channel script
1. No setup needed, leave as is

## Outputs script
1. No setup needed, leave blank

## Provider script
1. Requirements:
    - Update name variable to match Terraform workspace name for this domain

## Variables script
1. No setup needed other than to ensure that variable names in the script match variable names defined in the Terraform workspace EXACTLY 

(end of section to delete once setup is complete)

# Terraform Scripts for this repo

<b>Overview</b>   
These scripts work in concert to define the logging metrics and alerts for the associated HIN domain. These scripts are accessed by the Terraform workflows contained within the repository to leverage Terraform to create the alerts in GCP and connect to the desired notification channel to make those alerts available to anyone who is subscribed.   
    
<b>What's In Each Script</b>
   
 1. cloud_composer_alerts.tf   
    This is where each of the desired alerts are configured. These alerts are for any Composer task failure in the domain's production and qa proc projects along with the start and end time of each load cycle for the domain's production proc project. The load cycle start/end time alerts are configured specifically for each load cycle of interest.
    Details about configuration for Google monitoring alerts using Terraform can be found here:    https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/monitoring_alert_policy
 2. logging_metrics.tf    
    This is where each of the desired logging metrics are configured. These metrics include the start and end time for load cycles within the domain's production proc project as well as Composer task failures for the domain's production and qa proc projects. 
    Details about configuration for Google logging metrics using Terraform can be found here:    https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/logging_metric
 3. main.tf   
    This script should remain blank, it allows for the tf-scoping-workflow to bypass TFLint.   
 4. notification_channel.tf   
    Notification channel configuration for alerts to go to PagerDuty. Details about this configuration can be found here:    
    https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/monitoring_notification_channel
 5. outputs.tf   
    This script should remain blank, it allows for the tf-scoping-workflow to bypass TFLint.   
 6. provider.tf   
    Provider configuration to allow Terraform to interact with GCP. More details about provider configuration can be found here:   
    https://developer.hashicorp.com/terraform/language/providers/configuration
 7. variables.tf   
    This script is where the variables needed in other scripts within the repo are instantiated. They need to exactly align with the naming conventions for each associated variable in the domain's monitoring Terraform workspace. More details about Terraform input variables can be found here:   
    https://developer.hashicorp.com/terraform/language/values/variables
