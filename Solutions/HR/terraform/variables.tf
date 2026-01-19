variable "project_id" {
  description = "GCP project ID"
}


variable "secrets_map" {
  description = "Map of secrets to list of users accessing them"
  type        = map(list(string))
  default     = { 
    "lawson_jdbc_pwd"               = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com:"],
    "ats_infor_credentials"         = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com"],
    "hca_hrg_hcaps_sftp_connection" = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com"],
    "landmark_db2_jdbc_pwd"         = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com"],
    "lawson_jdbc_pwd"               = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com"],
    "rdm_jdbc_pwd"                  = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com"],
    "synapse_jdbc_pwd"              = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com"],
    "tms_sftp_connection"           = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com"],
    "enwisen_sftp_connection"       = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com"],
    "hca_hrg_precheck_ftp_connection" = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com"],
    "infor_sftp_passwd"             = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com"],
    "enwisen_pgp_public_key"        = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com"],
    "enwisen_pgp_private_key"       = ["serviceAccount:df-atos@hca-hin-prod-proc-hr.iam.gserviceaccount.com"],
  }
}

variable "lawson_jdbc_pwd" {
  description = "Secret data for lawson"
  sensitive   = true
}

variable "ats_infor_credentials" {
  description = "Secret data for ats_infor_credentials"
  sensitive   = true
}

variable "hca_hrg_hcaps_sftp_connection" {
  description = "Secret data for hca_hrg_hcaps_sftp_connection"
  sensitive   = true
}

variable "landmark_db2_jdbc_pwd" {
  description = "Secret data for landmark_jdbc_pwd"
  sensitive   = true
}

variable "rdm_jdbc_pwd" {
  description = "Secret data for rdm_jdbc_pwd"
  sensitive   = true
}

variable "synapse_jdbc_pwd" {
  description = "Secret data for synapse_jdbc_pwd"
  sensitive   = true
}

variable "tms_sftp_connection" {
  description = "Secret data for tms_jdbc_pwd"
  sensitive   = true
}

variable "enwisen_sftp_connection" {
  description = "Secret data for tms_jdbc_pwd"
  sensitive   = true
}

variable "hca_hrg_precheck_ftp_connection" {
  description = "Secret data for hca_hrg_precheck_ftp_connection"
  sensitive   = true
}

variable "infor_sftp_passwd" {
  description = "Secret data for infor_sftp_passwd"
  sensitive   = true
}

variable "enwisen_pgp_public_key" {
  description = "Secret data for enwisen_pgp_public_key"
  sensitive   = true
}

variable "enwisen_pgp_private_key" {
  description = "Secret data for enwisen_pgp_private_key"
  sensitive   = true
}