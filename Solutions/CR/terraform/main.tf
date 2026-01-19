terraform {
  backend "gcs" {
    prefix  = "terraform/backend/"
  }
}

provider "google" {
  project     = var.project_id
}

###  Secrets for us-east4 region  ###
resource "google_secret_manager_secret" "tf_gsm" {
  for_each  = var.secrets_map
  project   = var.project_id
  secret_id = each.key
  replication {
    user_managed {
      replicas {
        location = "us-east4" # Confirm this is correct then delete this comment
      }
      replicas {
        location = "us-central1" # Confirm this is correct then delete this comment
      }
    }
  }
}

resource "google_secret_manager_secret_iam_binding" "gsm_member" {
  for_each  = var.secrets_map
  project   = var.project_id
  secret_id = google_secret_manager_secret.tf_gsm[each.key].secret_id
  role      = "roles/secretmanager.secretAccessor"
  members   = each.value
}

#add additional variables below, an example is included for reference 
/*resource "google_secret_manager_secret_version" "facsched" {                  # Use a shorthand of the full variable name      
  secret      = google_secret_manager_secret.tf_gsm["facsched_sql_jdbc_pwd"].id # Use the same variable name as defined in variables.tf within the quotes
  secret_data = var.facsched_sql_jdbc_pwd                                       # Use the same variable name as defined in variables.tf after var.
}*/

