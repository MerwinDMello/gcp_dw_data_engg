## Using Terraform and Google Secret Manager (GSM) module to create secrets
<details>
    <summary><b>Overview</b></summary>

We needed a way to populate secrets inside GSM without having console or API access to GCP, and the only entrypoint to GSM is through service accounts used by GitHub Actions. The GSM workflow makes use of Terraform to create secrets in GCP so that these secrets are availble for use by Cloud Composer.

 </details>  

<details>
    <summary><b>How It Works</b></summary>
    
1. The variables.tf and main.tf script work in concert to:
    - Create defined secrets
    - Associate them with the designated service account
    - Apply the appropriate configuration to the secrets
2. The secrets are then referenced in the GSM workflow in order to save them to GCP for the project associated with the branch that is in use (dev, qa, prd)

### What secrets to create
The secrets are created using the `google_secret_manager_secret` resource.
The secrets ids are defined in the variable `secrets_map`, that maps the secret ids to the user or service accounts that have access to them.
The value of the secrets are read from the individual variables that are named the same as the secret ids.
For example `lawson_jdbc_pwd` is the variable that will hold the actual value of the secret, and `secrets_map` defined below
will create the secret id in GSM and assign the service account secret accessor role to be able to read the secret.
You can assign multiple service or user accounts as accessors to each secret defined in the map.
```hcl
variable "secrets_map" {
  default = {
    "lawson_jdbc_pwd" = ["serviceAccount:df-atos@hca-hin-qa-proc-hr.iam.gserviceaccount.com:"],
  }
}
```

### Using GitHub Actions
The deployment to dev/qa/prod is executed using Github actions (see the `gsm.yaml` file in `.github/workflows` directory).

The initial secrets are injected using Github [repository secrets](https://github.com/Azure/actions-workflow-samples/blob/master/assets/create-secrets-for-GitHub-workflows.md).
This is the only time the secrets are visible to whomever is performing this action.
After populating the secrets inside Github, you can now reference them using `${{ secrets.SECRET_NAME }}` syntax in the workflow script.
Terraform can read values for its variables if you export the variable names prefixed with `TF_VAR_`.
For example the value for `lawson_jdbc_pwd` variable in Terraform can be populated as follows:
```bash
export TF_VAR_lawson_jdbc_pwd="some-value"
```

Looking at the gsm workflow script, you see that all secrets are read from Github using the above convention.
</details>  

<details>
    <summary><b>Usage</b></summary>

Any secrets that are needed in GCP must be added in multiple locations in order to be created in GSM. These locations are:   
- GitHub Secrets in this repo   
- The main.tf script   
- The variables.tf script   
- The gsm.yaml script   
    
The same name must be utilized for a given secret in multiple places in order for the different pieces to work in concert:   
- In the main.tf script for the associated resource entry in the secret and secret_data values   
- In the variables.tf script in the associated "secrets_map" entry for the variable name in quotes   
- In the variables.tf script in the associated variable entry for the variable name in quotes
- In the gsm.yaml script preceded by TF_VAR_   
</details>
