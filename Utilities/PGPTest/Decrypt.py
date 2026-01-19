import pgpy
import shutil
import six
import io

# Load passphrase from file
   
# Load private key
PRIVATE_KEY_FILE = 'hca_gcp_prod-private.asc'
prv_key, _ = pgpy.PGPKey.from_file(str(PRIVATE_KEY_FILE))
# Unlock private key
# print(prv_key.is_protected)  # Should be True

# with prv_key.unlock('welcome'):#to be stored in secret manager
    # print(prv_key.is_unlocked)  #Should be True


    # Decrypt file
message_from_file = pgpy.PGPMessage.from_file('Onboarding_Audit_Report_1.csv.pgp')
# decrypted_f_t_e = prv_key.decrypt(message_from_file)
# f = open ('HCA_CM5Tickets_20230316.txt.pgp.decrypt','w')
# f.write(decrypted_f_t_e.message)
# f.close
decrypted_data = prv_key.decrypt(message_from_file).message
toread = io.BytesIO()
toread.write(bytes(decrypted_data))
toread.seek(0)
with open('Onboarding_Audit_Report_1.csv.pgp.decrypt', 'wb') as f:
    shutil.copyfileobj(toread, f)
    f.close()
# print('decrypted_f_t_e.is_encrypted')
# print(decrypted_f_t_e.is_encrypted)

    
