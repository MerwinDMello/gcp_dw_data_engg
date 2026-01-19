from google.cloud import kms_v1
from google.cloud import secretmanager_v1
from google.api_core.exceptions import AlreadyExists, PermissionDenied
import argparse
import base64
import crcmod
import six
import json
import os
import sys

# Set up the client for KMS and Secret Manager
kms_client = kms_v1.KeyManagementServiceClient()
secret_client = secretmanager_v1.SecretManagerServiceClient()

# Define the project ID and keyring parameters

# Create a keyring - COMMENTED OUT: Using pre-existing keyrings only
# def create_keyring(project_id, location_id, key_ring_id):
#     try:
#         keyring = kms_client.create_key_ring(parent=f"projects/{project_id}/locations/{location_id}", key_ring_id=key_ring_id)
#         print(f"Keyring created: {keyring.name}")
#     except AlreadyExists:
#         print(f"Keyring already exists: projects/{project_id}/locations/{location_id}/keyRings/{key_ring_id}")
#     except PermissionDenied as e:
#         print(f"Permission denied to create keyring. This is expected in enterprise environments where keyrings are pre-created.")
#         print(f"Assuming keyring exists: projects/{project_id}/locations/{location_id}/keyRings/{key_ring_id}")
#         # Verify the keyring actually exists by trying to access it
#         if not check_keyring_existence(project_id, location_id, key_ring_id):
#             print(f"ERROR: Keyring does not exist and cannot be created due to permissions: {str(e)}")
#             raise e
#     except Exception as e:
#         print(f"Unexpected error creating keyring: {str(e)}")
#         raise e

# Create a key - COMMENTED OUT: Using pre-existing keys only
# def create_key(project_id, location_id, key_ring_id, key_id):
#     try:
#         parent = kms_client.key_ring_path(project_id, location_id, key_ring_id)
#         crypto_key = kms_v1.types.CryptoKey(purpose="ENCRYPT_DECRYPT")
#         key = kms_client.create_crypto_key(parent=parent, crypto_key_id=key_id, crypto_key=crypto_key)
#         print(f"Key created: {key.name}")
#     except AlreadyExists:
#         print(f"Key already exists: projects/{project_id}/locations/{location_id}/keyRings/{key_ring_id}/cryptoKeys/{key_id}")
#     except PermissionDenied as e:
#         print(f"Permission denied to create key. Check that the service account has 'Cloud KMS CryptoKey Creator' role.")
#         print(f"Error details: {str(e)}")
#         raise e
#     except Exception as e:
#         print(f"Error creating key: {str(e)}")
#         raise e

# Check if the key exists
def check_secret_existence(project_id, secret_id):
    
    try:
        secret_name = secret_client.secret_path(project_id, secret_id)
        response = secret_client.get_secret(request={"name": secret_name})
        print(f"secret exists: {response.name}")
        return True
    except Exception as e:
        print(f"secret does not exist: {str(e)}")
    return False


def create_secret(project_id, secret_id, region="us-east4"):
    if not check_secret_existence(project_id, secret_id):
        secret = secret_client.create_secret(
            parent=f"projects/{project_id}",
            secret_id=secret_id,
            secret={"replication": {"user_managed": {"replicas":[{"location": region}]}}},
        )
        print(f"Secret created: {secret.name}")

def add_secret_version(project_id, secret_id, plaintext):
    secret_name = secret_client.secret_path(project_id, secret_id)
    secret_version = secret_client.add_secret_version(
        parent=secret_name,
        payload={"data": plaintext}
    )
    print(f"Secret version added: {secret_version.name}")

def crc32c(data):
    """
    Calculates the CRC32C checksum of the provided data.


    Args:
        data: the bytes over which the checksum should be calculated.


    Returns:
        An int representing the CRC32C checksum of the provided bytes.
    """
    import crcmod  # type: ignore
    import six  # type: ignore


    crc32c_fun = crcmod.predefined.mkPredefinedCrcFun("crc-32c")
    return crc32c_fun(six.ensure_binary(data))


# Check if the keyring exists  
def check_keyring_existence(project_id, location_id, key_ring_id):
    keyring_name = kms_client.key_ring_path(project_id, location_id, key_ring_id)
    try:
        response = kms_client.get_key_ring(name=keyring_name)
        print(f"Keyring exists: {response.name}")
        return True
    except Exception as e:
        print(f"Keyring does not exist: {str(e)}")
    return False

# Check if the key exists
def check_key_existence(project_id, location_id, key_ring_id, key_id):
    key_name = kms_client.crypto_key_path(project_id, location_id, key_ring_id, key_id)
    try:
        response = kms_client.get_crypto_key(name=key_name)
        print(f"Key exists: {response.name}")
        return True
    except Exception as e:
        print(f"Key does not exist: {str(e)}")
    return False

# Encrypt a string using the KMS key and store it in Secret Manager
def encrypt_and_store_string(project_id, location_id, key_ring_id, key_id, plaintext):
    
    plaintext_bytes = plaintext.encode("utf-8")
    plaintext_crc32c = crc32c(plaintext_bytes)
    key_name = kms_client.crypto_key_path(project_id, location_id, key_ring_id, key_id)

    response = base64.b64encode(kms_client.encrypt(request={
        "name": key_name,
        "plaintext": plaintext_bytes,
        "plaintext_crc32c": plaintext_crc32c,
    }).ciphertext)

    print(response)

    return response



if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--secret_id', type=str, help='secret id')
    parser.add_argument('--project_id', type=str, help='Project ID')
    parser.add_argument('--encrypt', type=str, help='encrypt')
    parser.add_argument('--region', type=str, default='us-east4', help='GCP region for KMS and Secret Manager')
    
    args = parser.parse_args()

    # Get secret values from environment variables (never from command line)
    # This avoids GitHub Actions command line masking of secret values
    # All secret values are required, so use os.environ[] to fail fast if missing
    encryption_key_arg = os.environ['encryption_key']
    key_prefix = os.environ.get('key_prefix', 'hin-key')  # Has fallback, so .get() is fine
    symmetric_key_arg = f"{key_prefix}-{encryption_key_arg}"

    username = ""
    password = ""
    conn = ""
    if args.encrypt == "false":
        secret_data = os.environ['secret_data']
        # Use us-east4 for Secret Manager (which requires zonal locations) regardless of KMS region
        create_secret(args.project_id, args.secret_id, "us-east4")
        add_secret_version(args.project_id, args.secret_id,secret_data.encode("utf-8"))
        sys.exit()
    else:
        secret_data = os.environ['secret_data']
        sd = json.loads(secret_data)
        user = sd['user']
        password = sd['password']
        conn = sd['connection']
    
    # DEBUG: Check if we're receiving literal *** values from env vars (not args)
    print(f"DEBUG: encryption_key from env: '{encryption_key_arg}' (length: {len(encryption_key_arg) if encryption_key_arg else 0})")
    print(f"DEBUG: symmetric_key from env: '{symmetric_key_arg}' (length: {len(symmetric_key_arg) if symmetric_key_arg else 0})")

    if "clinical" in args.project_id:
        symmetric_key_ring = "hin-clinical-keyring"
        symmetric_key = "hin-clinical-key"  # Use hardcoded clinical key name
        print(f"Using clinical keyring: {symmetric_key_ring}")
        print(f"Using clinical key: {symmetric_key}")
    else:
        symmetric_key_ring = f"hin-keyring-{encryption_key_arg}"
        symmetric_key = symmetric_key_arg  # Use the provided symmetric key

    print(f"DEBUG: Final symmetric_key_ring: '{symmetric_key_ring}'")
    print(f"DEBUG: Final symmetric_key: '{symmetric_key}'")
    print(f"Key used for encryption: {symmetric_key}/{symmetric_key_ring}")
    
    # Skip keyring check - service account doesn't have cloudkms.keyRings.get permission
    # Instead, directly check if the key exists (which implies keyring exists)
    print(f"Checking if KMS key exists: {symmetric_key} in keyring {symmetric_key_ring}")
    
    # Check if key exists - MUST exist, no creation allowed
    key_exists = check_key_existence(args.project_id, args.region, symmetric_key_ring, symmetric_key)
    print(f"Key exists: {key_exists}")
    if not key_exists:
        print(f"ERROR: Key {symmetric_key} does not exist in keyring {symmetric_key_ring}")
        print(f"Please ensure both the keyring and key exist before running this script.")
        sys.exit(1)
    
    username_encrypted = encrypt_and_store_string(args.project_id, args.region, symmetric_key_ring, symmetric_key, user).decode("utf-8")
    print(f"Username encrypted: {username_encrypted}")
    password_encrypted = encrypt_and_store_string(args.project_id, args.region, symmetric_key_ring, symmetric_key, password).decode("utf-8")
    print(f"Password encrypted: {password_encrypted}")
    conn_encrypted = encrypt_and_store_string(args.project_id, args.region, symmetric_key_ring, symmetric_key, conn).decode("utf-8")
    print(f"Connection encrypted: {conn_encrypted}")
    creds = {
        "user": username_encrypted,
        "password": password_encrypted,
        "connection": conn_encrypted
    }
    # Use us-east4 for Secret Manager (which requires zonal locations) regardless of KMS region
    create_secret(args.project_id, args.secret_id, "us-east4")
    add_secret_version(args.project_id, args.secret_id, json.dumps(creds).encode("utf-8"))
