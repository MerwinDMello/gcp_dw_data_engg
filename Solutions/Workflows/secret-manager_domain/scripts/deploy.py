from google.cloud import kms_v1
from google.cloud import secretmanager_v1
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

# Create a keyring
def create_keyring(project_id, location_id, key_ring_id):
    keyring = kms_client.create_key_ring(parent=f"projects/{project_id}/locations/{location_id}", key_ring_id=key_ring_id)
    print(f"Keyring created: {keyring.name}")

# Create a key
def create_key(project_id, location_id, key_ring_id, key_id):
    parent = kms_client.key_ring_path(project_id, location_id, key_ring_id)
    crypto_key = kms_v1.types.CryptoKey(purpose="ENCRYPT_DECRYPT")
    key = kms_client.create_crypto_key(parent=parent, crypto_key_id=key_id, crypto_key=crypto_key)
    print(f"Key created: {key.name}")

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


def create_secret(project_id, secret_id):
    if not check_secret_existence(project_id, secret_id):
        secret = secret_client.create_secret(
            parent=f"projects/{project_id}",
            secret_id=secret_id,
            secret={"replication": {"user_managed": {"replicas":[{"location": "us-east4"}]}}},
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
    parser.add_argument('--encryption_key', type=str, help='encryption_key')
    parser.add_argument('--domain', type=str, help='domain')
    
    args = parser.parse_args()

    username = ""
    password = ""
    conn = ""
    if args.encrypt == "false":
        secret_data = os.environ['secret_data']
        create_secret(args.project_id, args.secret_id)
        add_secret_version(args.project_id, args.secret_id,secret_data.encode("utf-8"))
        sys.exit()
    else:
        secret_data = os.environ['secret_data']
        sd = json.loads(secret_data)
        user = sd['user']
        password = sd['password']
        conn = sd['connection']
    
    symmetric_key = f"hin-{args.domain}-key-{args.encryption_key}"
    symmetric_key_ring = f"hin-keyring-{args.encryption_key}"
    print(f"Key used for encryption: {symmetric_key}/{symmetric_key_ring}")
    print(check_key_existence(args.project_id, "us-east4", symmetric_key_ring, symmetric_key))
    if not check_key_existence(args.project_id, "us-east4", symmetric_key_ring, symmetric_key):
        create_keyring(args.project_id, "us-east4", symmetric_key_ring)
        create_key(args.project_id, "us-east4", symmetric_key_ring, symmetric_key)
    
    username_encrypted = encrypt_and_store_string(args.project_id, "us-east4", symmetric_key_ring, symmetric_key, user).decode("utf-8")
    print(f"Username encrypted: {username_encrypted}")
    password_encrypted = encrypt_and_store_string(args.project_id, "us-east4", symmetric_key_ring, symmetric_key, password).decode("utf-8")
    print(f"Password encrypted: {password_encrypted}")
    conn_encrypted = encrypt_and_store_string(args.project_id, "us-east4", symmetric_key_ring, symmetric_key, conn).decode("utf-8")
    print(f"Connection encrypted: {conn_encrypted}")
    creds = {
        "user": username_encrypted,
        "password": password_encrypted,
        "connection": conn_encrypted
    }
    create_secret(args.project_id, args.secret_id)
    add_secret_version(args.project_id, args.secret_id, json.dumps(creds).encode("utf-8"))
