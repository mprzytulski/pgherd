import os
from Crypto.PublicKey import RSA

key = RSA.generate(2048, os.urandom)
print key.exportKey('OpenSSH')

f = open('mykey.pem','w')
f.write(key.exportKey('PEM'))
f.close()
