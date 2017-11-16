## Kerberos support installation

run the following in addition to the typical installation instructions

`pip install -r ./spot-ingest/kerberos-requirements`


## Ingest.conf JSON file

sample below:

```
"kerberos":{
      "enabled": "true",
      "principal": "spot_admin",
      "keytab": "/opt/security/spot_admin.keytab",
      "sasl_mech":"GSSAPI",
      "security_proto":"SASL_PLAINTEXT",
      "min_relogin": 6000
    },
    "ssl": {
      "ca_location": "",
      "cert_location": "",
      "key_location": ""
```

Please see [LIBRDKAFKA Configurations](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
for reference

