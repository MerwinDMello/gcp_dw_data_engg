from google.cloud import secretmanager_v1
import argparse

# Set up the client for Secret Manager
secret_client = secretmanager_v1.SecretManagerServiceClient()

# Check if the secret exists
def check_secret_existence(project_id, secret_id):
    try:
        secret_name = secret_client.secret_path(project_id, secret_id)
        response = secret_client.get_secret(request={"name": secret_name})
        print(f"secret exists: {response.name}")
        return True
    except Exception as e:
        print(f"secret does not exist: {str(e)}")
    return False

# Check if the secret version exists
def check_version_existence(project_id, secret_id, name):
    parent = secret_client.secret_path(project_id, secret_id)
    if check_secret_existence(project_id, secret_id):
        try:
            secret_versions = secret_client.list_secret_versions(request={"parent": parent})
            for version in secret_versions:
                print(f"Checking version: {version.name}")  # Debug log
                # Extract the numeric project ID from version.name
                normalized_name = name.replace(project_id, version.name.split('/')[1])
                print(f"Normalized name: {normalized_name}")  # Debug log
                if version.name.strip().lower() == normalized_name.strip().lower():
                    print(f"Version exists: {version.name}")  # Debug log
                    return True
                else:
                    print(f"Version does not match: {version.name}")
        except Exception as e:
            print(f"Error checking secret version existence: {str(e)}")
    return False

# Disable a version of a secret
def disable_secret_version(project_id, secret_id, version_id):
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    print(f"Disabling secret version: {name}")  # Debug log
    print(f"Using project ID: {project_id}")  # Debug log

    # Check if the version exists
    if check_version_existence(project_id, secret_id, name):
        # try and disable the secret version
        try:
            response = secret_client.disable_secret_version(request={"name": name})
            print(f"Disabled secret version: {response.name}")
        except Exception as e:
            print(f"Error disabling secret version: {str(e)}")

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--secret_id', type=str, help='secret id')
    parser.add_argument('--project_id', type=str, help='Project ID')
    parser.add_argument('--version_id', type=str, help='version_id')

    args = parser.parse_args()

    disable_secret_version(args.project_id, args.secret_id, args.version_id)
