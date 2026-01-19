from google.cloud import secretmanager_v1
import argparse

# Set up the client for Secret Manager
secret_client = secretmanager_v1.SecretManagerServiceClient()

# Define the project ID and keyring parameters

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
                if version.name == name:
                    return True
                print(f"Version {version.name} exists")
        except Exception as e:
            print(f"secret version does not exist: {str(e)}")
    return False

# Delete a secret
def delete_secret(project_id, secret_id):
    # Check if the secret to be deleted actually exists
    if check_secret_existence(project_id, secret_id):
        print(f"Getting Name")
        name = secret_client.secret_path(project_id, secret_id)
        print(f"Finished Getting Name")
        # Try to delete the secret
        try: 
            print(f"Preparing to delete: {name}")
            secret_client.delete_secret(request={"name": name})
            print(f"Secret deleted: {name}")
        except Exception as e:
            print(f"Error deleting secret: {str(e)}")
    else:
        print(f"Secret Existance False")

# Delete a version of a secret
def delete_secret_version(project_id, secret_id, version_id):
    # First check if the secret exists
    if not check_secret_existence(project_id, secret_id):
        print(f"Cannot delete version - secret does not exist: {secret_id}")
        return
    
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    print(f"Attempting to delete version with name: {name}") # Added print
    try:
        response = secret_client.destroy_secret_version(request={"name": name})
        print(f"Successfully deleted secret version: {response.name}")
    except Exception as e:
        print(f"Caught an error during deletion.") # Added print
        print(f"Error deleting secret version: {str(e)}")
        print(f"Error type: {type(e).__name__}")
        if hasattr(e, 'code'):
            print(f"Error code: {e.code}")
        if hasattr(e, 'details'):
            # This is likely where the real problem is
            print(f"Error details: {e.details}") 
        # Re-raise the exception to stop execution and see the full traceback
        raise e

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--secret_id', type=str, help='secret id')
    parser.add_argument('--project_id', type=str, help='Project ID')
    parser.add_argument('--version_id', type=str, help='version_id')

    args = parser.parse_args()

    print(f"Starting deletion with arguments:")
    print(f"  Project ID: {args.project_id}")
    print(f"  Secret ID: {args.secret_id}")
    print(f"  Version ID: {args.version_id}")

    if args.version_id == 'null' or args.version_id == '' or args.version_id is None:
        print("Deleting entire secret (no version specified)")
        delete_secret(args.project_id, args.secret_id)
    else:
        print(f"Deleting specific version: {args.version_id}")
        delete_secret_version(args.project_id, args.secret_id, args.version_id)

    print("Script execution completed.")
