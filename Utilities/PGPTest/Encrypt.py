import pgpy
import shutil
import six

PUBLIC_KEY_FILE = 'hca_gcp_prod-public.asc'    
pub_key, _ = pgpy.PGPKey.from_file(str(PUBLIC_KEY_FILE))
    
   
    # Encrypt file
f_t_e = pgpy.PGPMessage.new(str('HCA_CM5Tickets_20230507.txt.pgp'),file=True)
encrypted_f_t_e = pub_key.encrypt(f_t_e)
f = open ('HCA_CM5Tickets_20230507.txt.pgp','w')
f.write(str(encrypted_f_t_e))
f.close

print('encrypted_f_t_e.is_encoded')
print(encrypted_f_t_e.is_encrypted)