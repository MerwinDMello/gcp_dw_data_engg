# Cloud Run Setup Composite Action

## Overview

This composite action prepares and processes configuration files for Google Cloud Run jobs and services in a GitHub Actions workflow. It detects changed or relevant Cloud Run job/service folders, extracts and merges environment and job-specific configuration, and outputs ready-to-use artifacts for downstream deployment steps. It supports both ETL and HIN2.0 repository structures and can be triggered by file changes or workflow dispatch.

## Steps

1. **Detect Relevant Cloud Run Folders**  
   - If triggered by workflow dispatch, collects all contents in the Cloud Run directory (excluding markdown files) for redeployment.
   - If triggered by a code change, uses `tj-actions/changed-files` to detect changed files in the Cloud Run directory (excluding markdown files).
2. **Process Changed Paths**  
   Deduplicates and extracts the relevant Cloud Run job/service folders from the changed or collected files for further processing.
3. **Prepare Environment Configuration**  
   - For ETL repos: Extracts environment variables from the appropriate environment config YAML for the branch and domain, and writes them to `env_config.yaml`.
   - For HIN2.0 repos: Extracts the section of the environments config YAML for the branch and writes it to `env_config.yaml`.
4. **Merge Job/Service Configurations**  
   For each relevant job/service folder, merges the job-specific config (if present) with the environment config, giving precedence to job-specific values. Outputs merged YAML and JSON files for each job/service.
5. **Create Artifacts and Matrix Output**  
   Stores all merged config files in an `artifacts` directory, converts them to JSON, and outputs a matrix of config file paths for use in downstream deployment steps.
6. **Upload Artifacts**  
   Uploads the `artifacts` directory as a GitHub Actions artifact for use in later workflow steps.

## Usage

Use this composite action at the start of your Cloud Run deployment workflow to automatically detect changed or relevant jobs/services, prepare their configuration, and output artifacts for downstream deployment steps. It supports both ETL and HIN2.0 repository structures and can be triggered by file changes or workflow dispatch.

**Inputs include:**
- `branch`: The branch/environment relevant to the workflow run (required).
- `dispatch`: Whether the workflow was triggered by a workflow dispatch event (required).
- `env_folder`: The name of the folder containing environment YAMLs.
- `cloud_run_path`: The path to the Cloud Run folder in the repository.
- `repo_type`: The caller repository type, either ETL or HIN2.0 (required).
- `domain`: The domain/subdomain of the caller repo, directs to the correct environments config file name (required).

**Outputs include:**
- `matrix`: Matrix of processed config file paths for downstream deployment steps.

# Cloud Run Deploy Composite Action

## Overview

This composite action automates the setup, build, and deployment of Google Cloud Run jobs and services using GitHub Actions. It handles authentication, environment preparation, Docker image build and push, YAML configuration generation, and deployment to Google Cloud Run. It supports both Cloud Run jobs and services, and can be used for internal, external, and job-based deployments.

## Steps

1. **Authenticate to Google Cloud**  
   Uses the `google-github-actions/auth` action to authenticate using Workload Identity Federation and a specified service account.
2. **Set Up Google Cloud SDK**  
   Installs the Google Cloud SDK using the `google-github-actions/setup-gcloud` action.
3. **Authenticate Docker with Artifact Registry**  
   Configures Docker to authenticate with Artifact Registry for the specified region and for `us-docker.pkg.dev`.
4. **Prepare Environment Configuration**  
   If an environment config file exists for the service, extracts the relevant section for the current branch and writes it to the service's config directory for use during deployment.
5. **Build and Push Docker Image**  
   Builds a Docker image from the specified source directory and pushes it to the provided container registry path.
6. **Deploy to Cloud Run Service (External/Service Type)**  
   - If the deployment type is `external` or `service`, checks if the service exists and deploys the new image using the `google-github-actions/deploy-cloudrun` action or via YAML configuration.
7. **Create and Deploy Cloud Run Job (Job Type)**  
   - If the deployment type is `job`, generates a YAML configuration for the Cloud Run job and deploys it using `gcloud run jobs replace`.
8. **Create and Deploy Cloud Run Service (Service Type)**  
   - If the deployment type is `service`, generates a YAML configuration for the Cloud Run service and deploys it using `gcloud run services replace`.
9. **Verify Deployment**  
   For external and service deployments, describes the deployed service and checks for readiness.

## Usage

Use this composite action in your workflow to automate the setup, build, and deployment of Cloud Run jobs and services. The action will handle authentication, environment setup, Docker image build, push, and deployment for both job and service types, ensuring your workloads are deployed to the correct environment.

**Inputs include:**
- `workload_identity_provider`: The workload identity provider for Google Cloud authentication.
- `service_account`: The service account to use for Google Cloud authentication.
- `branch`: The branch being deployed (e.g., dev, qa, prod).
- `cloud_run_path`: The path to the Cloud Run folder in the repository.
- `image_path`: The full path to the Docker image, including the tag.
- `src`: The source directory for the service being deployed.
- `type`: The type of cloud run deployment (`job`, `service`, or `external`).
- `job_name`: The name of the Cloud Run job or service to create or update.
- `region`: The GCP region where the Cloud Run job or service will be deployed.
- `vpc_connector`: The VPC connector to use for the Cloud Run job or service.
- `memory`: The memory limit for the Cloud Run job or service container.
- `cpu`: The CPU limit for the Cloud Run job or service container.
- `deploy_service_account`: The service account to use for the Cloud Run job or service.
- `timeout`: The timeout for the Cloud Run job or service in seconds.
- `retries_number`: The maximum number of retries for the Cloud Run job.
- `proc_project`: The relevant proc project name.
