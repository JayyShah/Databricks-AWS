# Create Metastore

import requests

url = "https://<workspace_url>/api/2.1/unity-catalog/metastores"

payload = "{\r\n  \"name\": \"fbd-metastore\",\r\n  \"storage_root\": \"s3://<created-S3-bucket>/\"\r\n}"
headers = {
  'Authorization': 'Select Basic Authorization',
  'Content-Type': 'text/plain'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)

# ============================================================================================================================

# Update Parameters in MetaStore

import requests

url = "https://<workspace_url>/api/2.1/unity-catalog/metastores/<metastore-id>"

payload = "{\r\n  \"name\": \"fbd-uc-metastore\"\r\n}"
headers = {
  'Authorization': 'Select Basic Authorization',
  'Content-Type': 'text/plain'
}

response = requests.request("PATCH", url, headers=headers, data=payload)

print(response.text)

# ===============================================================================================================================

# Delete a MetaStore 


import requests

url = "https://<workspace_url>/api/2.1/unity-catalog/metastores/<metastore-id>"

payload = "{\"force\":\"true\"}"
headers = {
  'Authorization': 'Select Basic Authorization',
  'Content-Type': 'text/plain'
}

response = requests.request("DELETE", url, headers=headers, data=payload)

print(response.text)

# ====================================================================================================================================

# Get Details about existing MetaStore

import requests

url = "https://<workspace_url>/api/2.1/unity-catalog/metastores/<metastore-id>"

payload = ""
headers = {
  'Authorization': 'Select Basic Authorization'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)

# ========================================================================================================================================


