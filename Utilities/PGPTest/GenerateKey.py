import pgpy
import shutil
import six

from pgpy.constants import (
    PubKeyAlgorithm, KeyFlags, HashAlgorithm,
    SymmetricKeyAlgorithm, CompressionAlgorithm)
from subprocess import Popen, PIPE
from tempfile import mkdtemp


def pgpy_create_key():
    # we can start by generating a primary key. For this example, we'll use RSA, but it could be DSA or ECDSA as well
    key = pgpy.PGPKey.new(PubKeyAlgorithm.RSAEncryptOrSign, 4096)

    # we now have some key material, but our new key doesn't have a user ID yet, and therefore is not yet usable!
    uid = pgpy.PGPUID.new('Pratyush Nayak', comment='Testing PGP ', email='Pratyush.Nayak@hcahealthcare.com')

    # now we must add the new user id to the key. We'll need to specify all of our preferences at this point
    # because PGPy doesn't have any built-in key preference defaults at this time
    # this example is similar to GnuPG 2.1.x defaults, with no expiration or preferred keyserver
    key.add_uid(uid, usage={KeyFlags.Sign, KeyFlags.EncryptCommunications, KeyFlags.EncryptStorage},
                hashes=[HashAlgorithm.SHA256, HashAlgorithm.SHA384, HashAlgorithm.SHA512, HashAlgorithm.SHA224],
                ciphers=[SymmetricKeyAlgorithm.AES256, SymmetricKeyAlgorithm.AES192, SymmetricKeyAlgorithm.AES128],
                compression=[CompressionAlgorithm.ZLIB, CompressionAlgorithm.BZ2, CompressionAlgorithm.ZIP, CompressionAlgorithm.Uncompressed])
    return key

if __name__ == '__main__':
    key = pgpy_create_key()
    # key.is_public is False
# key.is_protected is False
    key.protect("welcome", SymmetricKeyAlgorithm.AES256, HashAlgorithm.SHA256)
# key.is_protected is now True
    print(key) 
    f = open ('./PGPTest/keys.txt','w')
    f.write(str(key))
    f.write(str(key.pubkey))
   
    f.close