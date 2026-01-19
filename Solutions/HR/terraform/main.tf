terraform {
  backend "gcs" {
    prefix  = "terraform/backend/"
  }
}

provider "google" {
  project     = var.project_id
}

###  Secrets for us-east4 region  ###
resource "google_secret_manager_secret" "hrg_gsm" {
  for_each  = var.secrets_map
  project   = var.project_id
  secret_id = each.key
  replication {
    user_managed {
      replicas {
        location = "us-east4"
      }
      replicas {
        location = "us-central1"
      }
    }
  }
}

resource "google_secret_manager_secret_iam_binding" "gsm_member" {
  for_each  = var.secrets_map
  project   = var.project_id
  secret_id = google_secret_manager_secret.hrg_gsm[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  members   = each.value
}

resource "google_secret_manager_secret_version" "ats" {
  secret      = google_secret_manager_secret.hrg_gsm["ats_infor_credentials"].id
  secret_data = var.ats_infor_credentials
}

resource "google_secret_manager_secret_version" "hca_hrg_hcaps" {
  secret      = google_secret_manager_secret.hrg_gsm["hca_hrg_hcaps_sftp_connection"].id
  secret_data = var.hca_hrg_hcaps_sftp_connection
}

resource "google_secret_manager_secret_version" "landmark_db2_jdbc" {
  secret      = google_secret_manager_secret.hrg_gsm["landmark_db2_jdbc_pwd"].id
  secret_data = var.landmark_db2_jdbc_pwd
}

resource "google_secret_manager_secret_version" "lawson" {
  secret      = google_secret_manager_secret.hrg_gsm["lawson_jdbc_pwd"].id
  secret_data = var.lawson_jdbc_pwd
}

resource "google_secret_manager_secret_version" "rdm_jdbc" {
  secret      = google_secret_manager_secret.hrg_gsm["rdm_jdbc_pwd"].id
  secret_data = var.rdm_jdbc_pwd
}

resource "google_secret_manager_secret_version" "synapse_jdbc" {
  secret      = google_secret_manager_secret.hrg_gsm["synapse_jdbc_pwd"].id
  secret_data = var.synapse_jdbc_pwd
}

resource "google_secret_manager_secret_version" "tms_sftp" {
  secret      = google_secret_manager_secret.hrg_gsm["tms_sftp_connection"].id
  secret_data = var.tms_sftp_connection
}

resource "google_secret_manager_secret_version" "enwisen_sftp" {
  secret      = google_secret_manager_secret.hrg_gsm["enwisen_sftp_connection"].id
  secret_data = var.enwisen_sftp_connection
}

resource "google_secret_manager_secret_version" "hca_hrg_precheck" {
  secret      = google_secret_manager_secret.hrg_gsm["hca_hrg_precheck_ftp_connection"].id
  secret_data = var.hca_hrg_precheck_ftp_connection
}

resource "google_secret_manager_secret_version" "infor_sftp_passwd" {
  secret      = google_secret_manager_secret.hrg_gsm["infor_sftp_passwd"].id
  secret_data = var.infor_sftp_passwd
}

resource "google_secret_manager_secret_version" "enwisen_pgp_public_key" {
  secret      = google_secret_manager_secret.hrg_gsm["enwisen_pgp_public_key"].id
  secret_data = var.enwisen_pgp_public_key
}

resource "google_secret_manager_secret_version" "enwisen_pgp_private_key" {
  secret      = google_secret_manager_secret.hrg_gsm["enwisen_pgp_private_key"].id
  secret_data = var.enwisen_pgp_private_key
}