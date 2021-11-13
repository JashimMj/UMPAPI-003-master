#!/usr/bin/env python3

import requests

cert_file_path = "cert.pem"
key_file_path = "key.pem"

cert = (cert_file_path, key_file_path)
URL = 'https://www.idra-ump.com/test/app/extern/monitor/health-check'
response = requests.get(URL, cert=cert)
result = response.text
print (result)


# https://stackoverflow.com/questions/17576324/python-requests-ssl-error-for-client-side-cert
