# Dataflow Setup Composite Action

## Overview

This composite action prepares and processes configuration files for Google Cloud Dataflow jobs in a GitHub Actions workflow. It detects changed Dataflow job files, extracts and merges relevant configuration sections, and outputs ready-to-use artifacts for downstream build and deploy steps. It is designed to work for Dataflow jobs leveraging Python, Java, or YAML SDK.

## Steps

1. **Detect Changed Dataflow Files**  
   Uses the `tj-actions/changed-files` action to identify which files in the Dataflow directory have changed in the current workflow run.
2. **Debug Changed Files**  
   Prints references and lists all changed files for traceability and debugging.
3. **Extract Changed Job Directories**  
   Processes the list of changed files to extract unique Dataflow job directories, ensuring only relevant jobs are considered for further processing.
4. **Prepare Environment Configuration**  
   - For ETL repositories: Extracts and writes environment variables from the appropriate environment config file (based on the relevant branch name) into `env_config.yaml`.
   - For HIN2.0 repositories: Extracts variables from the section of the single environments config file that is relevant for the current branch and writes them to the file named  `env_config.yaml`.
5. **Fetch and Merge Job Configurations**  
   For each changed job:
   - Locates the job’s config file.
   - Extracts the build section and the environment section for the current branch.
   - Merges these with the environment config into a single YAML file for the job.
   - If a job config is missing, uses only the environment config and issues a warning.
6. **Convert and Store Artifacts**  
   - Converts each merged YAML config to JSON and stores it in a `dataflow_artifacts` directory.
   - Collects all config file paths into a JSON array and saves it as `matrix.json` in a `matrix_artifacts` directory.
7. **Upload Artifacts**  
   - Uploads the `dataflow_artifacts` directory for use in later workflow steps.
   - Uploads the `matrix_artifacts/matrix.json` file, which can be used as a matrix for parallelized build or deploy jobs.

## Usage

Use this composite action to start the process of deploying Dataflow code from your GitHub repository. The composite action serves to automatically detect changed jobs, prepare their configuration, and output artifacts for downstream build and deployment steps.  
It supports both ETL and HIN2.0 repository structures and ensures that only relevant jobs are processed based on file changes.

**Inputs include:**
- `branch`: The relevant branch name.
- `subdomain`: The relevant subdomain
- `dataflow_path`: Path to the Dataflow directory
- `repo_type`: The type of repository (`ETL` or `HIN2.0`).
- `env_folder`: Folder containing environment configuration files

# Dataflow Build Flex Template Composite Action

## Overview

This composite action automates the build process for Google Cloud Dataflow Flex Template jobs using GitHub Actions. It supports both Python and Java Dataflow jobs, handling environment setup, authentication, Docker image building, Maven packaging, and template creation.

## Steps

1. **Checkout Repository**  
   Checks out the specified repository containing the Dataflow job source code.
2. **Install and Setup SDKs**  
   - For Java jobs: Installs Maven and sets up Java 11 using Temurin.
   - For Python jobs: Proceeds directly to authentication and build steps.
3. **Authenticate to Google Cloud**  
   Authenticates using Workload Identity Federation and a specified service account.
4. **Set Up Google Cloud SDK**  
   Installs the Google Cloud SDK. For Java jobs, also installs the `beta` components.
5. **Authenticate Docker with Artifact Registry**  
   - For Python jobs: Configures Docker to authenticate with Artifact Registry using `gcloud`.
   - For Java jobs: Configures Docker authentication using `gcloud`.
6. **Build and Push Docker Image**  
   - For Python jobs: Builds a Docker image from the job source and pushes it to the specified container registry.
7. **Build Dataflow Flex Template**  
   - For Python jobs: Uses `gcloud dataflow flex-template build` to create the template, specifying the image, SDK language, and metadata file.
   - For Java jobs: Runs Maven commands to build and package the job, then builds the Flex Template using `gcloud`.

## Usage

Use this composite action in your workflow to automate the build and packaging of Dataflow Flex Template jobs. The action will handle all necessary setup, authentication, and build steps for both Python and Java jobs, preparing your templates for deployment.

**Inputs include:**
- Repository and job folder details
- SDK language (Python or Java)
- Docker image and template paths
- Google Cloud authentication details
- Java/Python-specific build parameters

# Dataflow Deploy Composite Action

## Overview

This composite action automates the deployment of Google Cloud Dataflow streaming jobs using GitHub Actions. It supports Python, Java, and YAML SDK Dataflow jobs, handling authentication, environment setup, and the appropriate deployment command for each job type.

## Steps

1. **Authenticate to Google Cloud**  
   Authenticates using Workload Identity Federation and a specified service account to enable secure access to Google Cloud resources.
2. **Set Up Google Cloud SDK**  
   - For Python jobs: Installs the standard Google Cloud SDK.
   - For Java jobs: Installs the Google Cloud SDK with the `beta` components.
3. **Deploy Python Dataflow Job**  
   - Checks if the Dataflow job already exists and determines if the `--update` flag should be used.
   - Constructs and runs the `gcloud dataflow flex-template run` command with all required parameters, including template location, region, subnetwork, service account, worker settings, and any additional parameters.
4. **Deploy Java Dataflow Job**  
   - Runs the `gcloud dataflow flex-template run` command with Java-specific parameters, including disabling public IPs, setting service account, parameters, worker settings, region, template location, and additional experiments.
5. **Resolve and Print YAML for YAML SDK Jobs**  
   - For YAML SDK jobs, dynamically replaces placeholders in the `pipeline.yaml` file using values from the provided Jinja inputs and the processing project name.
   - Outputs the resolved YAML to `resolved_pipeline.yaml` for deployment.
6. **Deploy YAML SDK Dataflow Job**  
   - Runs the `gcloud dataflow yaml run` command using the resolved YAML file and passes all required pipeline options and parameters for the job.

## Usage

Use this composite action in your workflow to automate the deployment of Dataflow streaming jobs. The action will handle authentication, environment setup, and the correct deployment command for Python, Java, or YAML SDK jobs.

**Inputs include:**
- Authentication details (workload identity provider, service account)
- Dataflow job configuration (job name, template path, region, subnetwork, service account for the job, worker settings, parameters)
- SDK language (Python, Java, or YAML)
- Jinja inputs for YAML SDK jobs
- Additional deployment arguments as needed