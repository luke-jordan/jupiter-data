# Auth Validator

This module serves as a layer to validate external requests.
This ensures that all requests coming into the system are from one systems and thus the request is secure to process.


The auth validator accepts two parameters: 
A `givenHash` and a `givenKey` , both parameters are `string` values.

The auth validator uses the `givenKey` to generate a `systemGeneratedHash` which is then compared to the `givenHash`,
if the `givenHash` and the `systemGeneratedHash` are the same value, then the request is valid and can be processed.

The `givenHash` should be generated with the algorithm:
givenHash = hmac_256(secret + givenKey)

The `secret` as mentioned above is stored (for staging and production) as a terraform variable in the google
cloud file: `https://console.cloud.google.com/storage/browser/_details/staging-terraform-state-bucket/jupiter-data-private-vars.tf?project=jupiter-ml-alpha`.
This secret is added to the config of the function by terraform when the Circle CI pipeline is running. 

Please refer to the `generateHash` function for explicit details about the construction of the hash 