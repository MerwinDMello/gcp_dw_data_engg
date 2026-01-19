variable "project_id" {
  description = "GCP project ID"
}

variable "secrets_map" {
  description = "Map of secrets to list of users accessing them"
  type        = map(list(string))
  default     = { 
    #add all variables and the associated service account below, an example is included for reference
    #"facsched_sql_jdbc_pwd"  = ["serviceAccount:gh-atos@hca-hin-qa-proc-ops.iam.gserviceaccount.com"],

  }
}

#define all variables below, one example is included for reference, note that you will use the same variable name in the main.tf script
/*variable "facsched_sql_jdbc_pwd" {
  description = "Secret data for Facsched SQL"
  sensitive   = true
}*/




