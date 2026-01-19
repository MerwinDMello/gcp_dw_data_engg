## Composer Workflows

### Build
**Composer Build Action**   
This GitHub action, named "Composer Build", is designed to set up the environment for deployment to Google Cloud Platform (GCP). It takes several inputs, converts YAML configuration to JSON, and then to environment variables.

**Inputs**
- `branch`: This determines the appropriate environment, based on the branch the PR will merge into
- `subdomain`: The subdomain for the GCS bucket and DAG folder
- `args`: Additional arguments for the build; this is optional and defaults to an empty string

**Outputs**   
- `infra_env_v_proc_project_id`: The project ID for the GCP setup.
- `infra_env_v_dag_bucket_name`: The name of the GCS bucket to sync the DAGs to.

**Steps**
1. **Setup environment**: This step sets up the environment configuration file and imports variables for the relevant environment.

2. **Convert YAML to JSON**: This step converts the YAML configuration file to JSON format.

3. **Convert JSON to variables**: This step uses the `rgarcia-phi/json-to-variables@v1.1.0` action to convert the JSON configuration to environment variables.

4. **Save Variables**: This step saves the variable values needed for the Deploy step.


### Deploy
**Composer Deploy Action**   
This GitHub action is designed to automate the process of syncing the dags folder to a Google Cloud Storage (GCS) bucket in Google Cloud Platform (GCP). It takes several inputs, performs authentication, sets up GCP, and completes the sync.

**Inputs**
- `branch`: This determines the appropriate environment, based on the branch the PR will merge into
- `subdomain`: The subdomain for the GCS bucket and DAG folder
- `bucket_name`: The name of the GCS bucket to sync the DAGs to
- `proc_project_id`: The project ID for the GCP setup
- `workload_identity_provider`: The workload identity provider for authentication, saved as a secret
- `service_account`: The service account for authentication, saved as a secret
- `args`: Additional arguments for the deployment; this is optional and defaults to an empty string

**Steps**
1. **Authenticate to Google Cloud**: 
   This step uses the google-github-actions/auth@v1.1.1 action to authenticate to the Google Cloud environment using the provided workload identity provider and service account.
2. **GCP setup**: This step uses the google-github-actions/setup-gcloud@v1 action to set up the GCP environment with the provided project ID.
3. **Sync repo with DAG**: This step syncs the repository with the GCS bucket. It uses the gcloud storage rsync command to recursively sync the subdomain directory within the dags folder with the corresponding directory in the GCS bucket. Note that objects not present in the GitHub version of the code are currently **not** deleted from the GCS bucket.