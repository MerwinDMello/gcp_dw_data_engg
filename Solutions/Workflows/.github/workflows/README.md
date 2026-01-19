## BigQuery Reusable Workflow

<details>
    <summary><b>Overview</b></summary>
    
This reusable GitHub Actions workflow is designed to build and deploy BigQuery resources. It includes steps to check out the repository, determine the domain if not provided, set a dispatch variable, and call composite actions for building and deploying BigQuery resources based on the repository type and branch.

</details>

<details>
    <summary><b>How It Works</b></summary>

**Prep Steps**
1. Check out repository: check out the caller repo to make its contents available to the workflow.
2. Determine domain: if the domain input is empty, this step uses a bash script to determine the repository's domain by searching for a matching file in the ./environments/ directory and extracting the subdomain from the filename. The extracted subdomain is then set as an output.
3. Determine dispatch: sets the dispatch variable to true if the workflow was manually triggered in the caller repo; otherwise, it sets the dispatch variable to false.

**Setup Step**   
The Setup step uses a [custom composite action](https://github.com/HCACloudDataEngineering/gcp-hin-worklows/blob/main/bigquery/setup/action.yaml) to build BigQuery resources, passing in the repository type, branch, domain, dispatch status, and GitHub token as inputs.

**Deploy Step**   
The Deploy step uses a [custom composite action](https://github.com/HCACloudDataEngineering/workflows/blob/main/bigquery/deploy/action.yaml) and associated [deployment script](https://github.com/HCACloudDataEngineering/gcp-hin-workflows/blob/main/bigquery/scripts/deploy.py) to deploy BigQuery resources, passing in the branch, domain, workload identity provider, service account, and default project ID as inputs.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow is called by HIN repositories to facilitate the automation of DDL deployment to create/update tables, views, routines, and udfs.

</details>

## Bigtable Reusable Workflow

<details>
    <summary><b>Overview</b></summary>

This reusable GitHub workflow orchestrates the setup and deployment of Google Cloud Bigtable resources. It is triggered by a `workflow_call` and prepares the environment by identifying changed or relevant Bigtable configuration files, processing and merging environment details, and generating artifacts for downstream deployment steps. The workflow supports both ETL and HIN2.0 repository structures.

</details>

<details>
    <summary><b>How It Works</b></summary>

**Setup Job**  
Calls the `HCACloudDataEngineering/gcp-hin-workflows/bigtable/setup@main` composite action, passing all relevant inputs. This step:
- Detects which Bigtable configuration files have changed or need to be redeployed.
- Processes and merges the relevant environment and Bigtable configuration files.
- Injects the correct GCP project ID into each Bigtable config file.
- Prepares artifacts for use in subsequent deployment jobs.

**Deploy Job*  
Runs the deployment using the `HCACloudDataEngineering/gcp-hin-workflows/bigtable/deploy@main` composite action. This step:
- Downloads the prepared artifacts and deployment scripts.
- Sets up the Python environment and installs required dependencies.
- Executes the deployment script to create or update Bigtable instances, tables, and app profiles as specified in the configuration files.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow is called by caller workflows to automate the setup and deployment of Bigtable resources, ensuring only relevant resources are processed based on file changes or workflow dispatch.

**Inputs include:**
- `branch`: The branch/environment relevant to the workflow run (required).
- `env_folder`: The name of the folder containing environment YAMLs.
- `repo_type`: The caller repository type, either ETL or HIN2.0 (required).
- `domain`: The domain/subdomain of the caller repo, directs to the correct environments config file name (required).
- `token`: The GitHub token for authentication (required).

</details>

## Cloud Run Deploy Reusable Workflow

<details>
    <summary><b>Overview</b></summary>

This reusable GitHub Actions workflow orchestrates the setup, build, and deployment of Google Cloud Run jobs and services. It is triggered by a `workflow_call` and prepares the environment by identifying changed or relevant Cloud Run job/service folders, processing and merging configuration files, and generating a matrix of configuration paths for downstream deployment steps. The workflow supports both ETL and HIN2.0 repository structures and can be triggered by file changes or workflow dispatch.
</details>

<details>
    <summary><b>How It Works</b></summary>

**Workflow Inputs**  
Accepts inputs for the branch/environment, dispatch status, environment folder, Cloud Run path, repository type (ETL or HIN2.0), and domain/subdomain. These inputs allow the workflow to be flexible and reusable across different repositories and environments.

**Setup Cloud Run Job**  
Calls the `HCACloudDataEngineering/gcp-hin-workflows/cloud-run/setup@main` composite action, passing all relevant inputs. This step:
- Detects which Cloud Run jobs/services have changed or need to be redeployed.
- Processes and merges the relevant environment and job/service configuration files.
- Prepares artifacts and a matrix of configuration paths for use in subsequent deployment jobs.

**Deploy Cloud Run Job**  
For each item in the manifest matrix, runs the deployment using the `HCACloudDataEngineering/gcp-hin-workflows/cloud-run/deploy@main` composite action. This step:
- Loads configuration values from the manifest and sets up environment variables for the deployment.
- Determines the deployment type (job or service) and defaults to job if not specified.
- Deploys the Cloud Run job or service using the correct configuration and authentication.
- Runs deployments in parallel for all relevant jobs/services, ensuring each is created or updated as specified in the configuration.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow is called by caller workflows to automate the setup and configuration of Cloud Run jobs and services, ensuring only relevant jobs/services are processed based on file changes or workflow dispatch.

**Inputs include:**
- `branch`: The branch/environment relevant to the workflow run (required).
- `dispatch`: Whether the workflow was triggered by a workflow dispatch event (required).
- `env_folder`: The name of the folder containing environment YAMLs.
- `cloud_run_path`: The path to the Cloud Run folder in the repository.
- `repo_type`: The caller repository type, either ETL or HIN2.0 (required).
- `domain`: The domain/subdomain of the caller repo, directs to the correct environments config file name (required).

</details>


## Composer Sync Reusable Workflow

<details>
    <summary><b>Overview</b></summary>
This reusable GitHub Actions workflow, triggered by a workflow_call, builds and synchronizes DAGs (Directed Acyclic Graphs) to a Google Composer bucket. It checks out the repository, sets up environment variables, calls a build action to prepare the DAGs, and then deploys (syncs) the DAGs to the specified Composer bucket using provided configurations and secrets.
</details>

<details>
    <summary><b>How It Works</b></summary>

**Setup Step**   
This step sets up the environment by iterating through YAML files in the specified environment folder, extracting the domain/subdomain name from the file name using a regular expression, and then storing the domain name in the GitHub Actions output.

**Build Step**   
The Build step uses a [custom composite action](https://github.com/HCACloudDataEngineering/gcp-hin-workflows/blob/main/composer/build/action.yaml) to set up the environment configuration for a specified branch and domain/subdomain by copying the relevant YAML file, converting it to JSON, and exporting its contents as environment variables. It then saves specific environment variables (infra_env_v_proc_project_id and infra_env_v_dag_bucket_name) as outputs for use in subsequent steps.

**Deploy Step**   
The Deploy step uses a [custom composite action](https://github.com/HCACloudDataEngineering/gcp-hin-workflows/blob/main/composer/deploy/action.yaml) to deploy DAGs (Directed Acyclic Graphs) to a Google Cloud Storage (GCS) bucket. It authenticates to Google Cloud using a specified workload identity provider and service account, sets up the Google Cloud CLI, and then synchronizes the local DAGs directory with the specified GCS bucket, optionally allowing for two-way synchronization and exclusion of specific paths.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow is called to provide ETL repositories with a straightforward way to facilitate syncing the files within the dags directory to the desired Composer bucket in GCS.

</details>

## Dataflow Setup Reusable Workflow

<details>
    <summary><b>Overview</b></summary>

This reusable GitHub Actions workflow is designed to orchestrate the setup phase for Google Cloud Dataflow jobs. It is triggered by a `workflow_call` and prepares the environment by identifying changed Dataflow job files, processing relevant configuration files, and generating a matrix of configuration paths for downstream build and deploy steps. The workflow supports both ETL and HIN2.0 repository structures.
</details>

<details>
    <summary><b>How It Works</b></summary>

**Workflow Inputs**  
Accepts inputs for the branch/environment, path to the Dataflow directory, repository type (ETL or HIN2.0), and the folder containing environment YAMLs. These inputs allow the workflow to be flexible and reusable across different repositories and environments.

**Checkout Repository**  
Uses the `actions/checkout` action to clone the repository so that all Dataflow job files and configuration files are available for processing.

**Setup Dataflow Step**  
Calls the `HCACloudDataEngineering/gcp-hin-workflows/dataflow/setup@main` composite action, passing all relevant inputs. This step:
- Detects which Dataflow files have changed.
- Processes and merges the relevant configuration files.
- Prepares artifacts and a matrix of configuration paths for use in subsequent workflow jobs.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow is called by caller workflows to automate the setup and configuration of Dataflow jobs, ensuring only relevant jobs are processed based on file changes.

**Inputs include:**
- `branch`: The branch/environment relevant to the workflow run (required).
- `dataflow_path`: The path to the Dataflow folder in the repository (default: `dataflow`).
- `repo_type`: The repository type, either `ETL` or `HIN2.0` (default: `HIN2.0`).
- `env_folder`: The folder containing environment YAML files (default: `environments`).

</details>

## Dataflow Build & Deploy Reusable Workflow

<details>
    <summary><b>Overview</b></summary>

This reusable GitHub Actions workflow orchestrates the build and deployment of Google Cloud Dataflow streaming jobs. Triggered by a `workflow_call`, it retrieves configuration artifacts from a prior setup workflow, builds Dataflow Flex Templates for Python and Java jobs, and deploys streaming Dataflow jobs using the appropriate SDK (Python, Java, or YAML). The workflow is designed for robust, matrix-driven parallelization and supports both ETL and HIN2.0 repository structures.
</details>

<details>
    <summary><b>How It Works</b></summary>

**Check and Download Artifacts Step**  
- Finds the most recent successful run of the Dataflow Setup Workflow associated with the pull request.
- Downloads the `matrix_artifacts` and `dataflow_artifacts` generated by the setup workflow.
- Extracts the matrix of configuration file paths (`matrix.json`) for use in the build and deploy jobs.

**Build Step**  
- Runs in parallel for each job configuration in the matrix.
- Checks out the repository and loads the relevant job configuration.
- Calls the `HCACloudDataEngineering/gcp-hin-workflows/dataflow/build@main` composite action to:
  - Build and push the Docker image for Python or Java Dataflow jobs.
  - Build the Dataflow Flex Template using the appropriate SDK and configuration.

**Deploy Step**  
- Runs in parallel for each job configuration in the matrix, after the build step.
- Loads the relevant job configuration and prepares any required Jinja variables for YAML SDK jobs.
- Calls the `HCACloudDataEngineering/gcp-hin-workflows/dataflow/deploy@main` composite action to:
  - Deploy the streaming Dataflow job using the correct SDK (Python, Java, or YAML).
  - Passes all required parameters, including authentication, job configuration, and runtime arguments.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow is called by other workflows to automate the build and deployment of Dataflow streaming jobs. It expects configuration artifacts from a prior setup workflow and supports matrix-driven parallelization for efficient processing of multiple jobs.

**Inputs include:**
- `branch`: The base branch/environment relevant to the workflow run (required).
- `dataflow_path`: The path to the Dataflow folder in the repository (required).

**Secrets include:**
- `gh_token`: The GitHub token for authentication (required).

</details>

## Data Quality DAG Sync Reusable workflow

<details>
    <summary><b>Overview</b></summary>
    
This reusable GitHub Actions workflow automates the detection, generation, and deployment of Data Quality (DQ) DAGs and associated spec files to Google Cloud Storage (GCS). It orchestrates the setup and deploy steps using custom composite actions, ensuring that only relevant DAGs are processed and deployed based on file changes or manual triggers.

</details>

<details>
    <summary><b>How It Works</b></summary>

**Setup Job**
1. **Check out repository:**  
   Checks out the caller repository to make its contents available to the workflow.
2. **Determine dispatch:**  
   Sets the dispatch variable to `true` if the workflow was manually triggered (`workflow_dispatch`); otherwise, sets it to `false`.
3. **Call dq_dags setup composite action:**
    The setup step uses a [custom composite action](https://github.com/HCACloudDataEngineering/gcp-hin-worklows/blob/main/dq_dags/setup/action.yaml) to detect changed usecase folders or collect all usecases for redeployment. It builds a matrix of usecases to process, outputting this matrix for downstream deployment.

**Deploy Job**  
1. **Check out repository:**  
   Checks out the caller repository to make its contents available to the workflow.
2. **Call dq_dags deploy composite action:**
    The deploy step uses a [custom composite action](https://github.com/HCACloudDataEngineering/gcp-hin-worklows/blob/main/dq_dags/deploy/action.yaml) to generate and upload DAGs and spec files for each use case in the matrix. It passes in the use case, branch, domain config path, DAG template paths, workload identity provider, and service account as inputs. The deploy step handles environment validation, DAG generation, authentication, and file uploads to GCS.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow should be called by a repository’s caller workflow to automate the detection, generation, and deployment of Data Quality DAGs.  
- **For incremental deployment:** Triggered by changes to non-markdown files within any `data_quality/usecase_*/` folder on the `dev`, `qa`, or `prod` branches.
- **For full redeployment:** Manually triggered via the Actions tab to redeploy all DAGs.

This ensures efficient, targeted, and automated deployment of Data Quality DAGs and spec files to GCS for Airflow and Dataplex consumption.

**Inputs passed from the caller to the reusable workflow:**
- `branch`: The branch/environment relevant to the workflow run (required)
- `domain_config_path`: Path to the domain configuration YAML file (required)
- `dag_template_create_update_path`: Path to the DAG template for create/update (required)
- `dag_template_delete_path`: Path to the DAG template for delete (required)
- `workload_identity_provider`: Workload identity provider for GCP authentication (required)
- `service_account`: Service account email for GCP authentication (required)

</details>

## Super Linter Reusable Workflow

<details>
    <summary><b>Overview</b></summary>

This reusable GitHub Actions workflow is designed to implement the super-linter to perform linting for Python. SQL and YAML code in a consistent manner across all repos within the organization.  
</details>

<details>
    <summary><b>How It Works</b></summary>

**Workflow Inputs**  
Accepts inputs for the release/version of the workflow from the `gcp-hin-workflows` repository.  This process is currently under review & not implemented on the repository yet. The default value for this input is set as the `main` branch.  The branch to use (until this has been merged) when calling this is `feature/ADO-24545/add_linting_workflow_and_config`.

**Checkout Repository**  
Uses the `actions/checkout` action to clone the repository so that all files are available for linting.

**Setup Git Step**  
This step is required to check out the necessary branch as the checkout checks out the repo in detached HEAD mode when triggered by a Pull Request.  The step:
- Sets the user name and email in the config 
- Performs a git checkout of the branch 
- Performs a pull on the branch to ensure it is up to date

**Remove local linter config Step**  
Checks to see if there is a local linters directory in the `.github` directory.  If such a directory exists, it will be removed.

**Checkout linter config Step**  
Uses the `actions/checkout` action to perform a sparse checkout on the `HCACloudDataEngineering/gcp-hin-workflows` to obtain the `HCACloudDataEngineering/gcp-hin-workflows/linters` directory and all files within it.
This allows for consistent linting across the organization.  This action also uses the workflow input, to determine the version of the linter config to clone.

**Prettier Ignore Step**  
In order to use the YAML validation and fix, super-linter uses Prettier.  There is a `.prettierignore` file within the linters directory which was checked out in the previous step.  This step moves the file from the linters directory into the `${{ github.workspace }}` where it will be referenced when the super-linter runs.
This will prevent the super-linter from running against the `.github` directory as this will affect workflows.

**Lint Code Base Step**  
This step is where the super-linter is called.  It uses `github/super-linter@v7` and has the following inputs set:
- FIX_SQLFLUFF: true
- FIX_PYTHON_BLACK: true
- FIX_YAML_PRETTIER: true
- VALIDATE_ALL_CODEBASE: false
- VALIDATE_PYTHON_BLACK: true
- VALIDATE_SQLFLUFF: true
- VALIDATE_YAML_PRETTIER: true

Currently this configuration allows validation and fixes to be applied to Python, SQL and YAML files.  

**Commit Super-Linter Changes Step**  
Checks for modified files using the `git status` command.  If there are modified files (i.e. super-linter has applied fixes to some of the tracked files), it adds those modified files, commits them and pushes back to the remote branch.

</details>

<details>
    <summary><b>Usage</b></summary>

This workflow is called by caller workflows to automate the super-linter linting process.

**Inputs include:**
- `release_version`: The release/version of the linters config to checkout (default: `main`).

</details>