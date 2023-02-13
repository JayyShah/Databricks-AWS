"""

Problem Statement: Write a helper function to create delta live table pipeline,
To handle multiple pipeline from a single notebook and also to perform CRUD operations 
on Pipelines.

"""

# Using Rest-Api(requests) method - Using the Dbutils method to get the context as a secret scope
# To use personal access token, host_name, host_token and cluster Id in a secret scope


import requests
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host_name = ctx.tags().get("browserHostName").get()
host_token = ctx.apiToken().get()
cluster_id = ctx.tags().get("clusterId").get()

response = requests.get(
    f'https://{host_name}/api/2.0/secrets/scopes/list',
    headers={'Authorization': f'Bearer {host_token}'}
  ).json()
scopes = dict([(s['name'], s.get('backend_type', 'DATABRICKS')) 
               for s in response['scopes']])
backend = scopes['personal_access_token']


""" 

Crud Operations:
	- Perform CRUD operations on Pipeline.


"""


"""

	1. Create a pipeline:
		 - Creates a pipeline for dlt task, in a notebook and returns a Pipeline ID for which it created.


"""
import requests
import json

url = f'https://{host_name}/api/2.0/pipelines'

payload = json.dumps({
  "name": "Full_load-Pipeline",
  "storage": "dbfs:/pipelines",
    "clusters": [
        {
            "autoscale": {
                "min_workers": 0,
                "max_workers": 1,
                "mode": "LEGACY"
            }
        }
    ],
    "photon": "true",
  "libraries": [
    {
      "notebook": {
        "path": "/path/to/your/dlt-notebook"
      }
    }
  ]
})
headers={'Authorization': f'Bearer {host_token}', 'Content-Type':'application/json'}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)




"""

	2. Get Details of any running Pipeline:
		 - Returns with the response of all the pararmeters of the pipeline and status of the pipeline.


"""

import requests

url = f'https://{host_name}/api/2.0/pipelines/{pipeline_id}'

payload={}
files={}
headers={'Authorization': f'Bearer {host_token}'}

response = requests.request("GET", url, headers=headers, data=payload, files=files)

print(response.text)



"""

	3. Edit a pipeline:
		- To edit any particulat details of pipeline (name, cluster config, path of notebook)

"""

import requests
import json

url = f'https://{host_name}/api/2.0/pipelines/{pipeline_id}'

payload = json.dumps({
  "storage": "dbfs:/pipelines",
  "libraries": [
    {
      "notebook": {
        "path": "/path/to/your/dlt-notebook"
      }
    }
  ]
})
headers={'Authorization': f'Bearer {host_token}', 'Content-Type':'application/json'}

response = requests.request("PUT", url, headers=headers, data=payload)

print(response.text)



"""

	4. Delete a pipeline:
			-  Deletes a particular pipeline 

"""


import requests

url = f'https://{host_name}/api/2.0/pipelines/{pipeline_id}'

payload={}
files={}
headers={'Authorization': f'Bearer {host_token}', 'Content-Type':'application/json'}

response = requests.request("DELETE", url, headers=headers, data=payload)

print(response.text)



"""

	5. Update any running Pipeline:
		- It takes parameters for Refreshing a running Pipeline as `refresh_selection`.
		- User can select multiple table to refresh to get the updated data in the pipeline

"""


import requests
import json

url = f'https://{host_name}/api/2.0/pipelines/{pipeline_id}/updates'

payload = json.dumps({
  "storage": "dbfs:/pipelines",
  "libraries": [
    {
      "notebook": {
        "path": "/path/to/your/dlt-notebook"
      }
    }
  ],
  "full_refresh": True
})
headers={'Authorization': f'Bearer {host_token}', 'Content-Type':'application/json'}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)



"""

	6. Status of the Update

"""


import requests
import json

url = f'https://{host_name}/api/2.0/pipelines/{pipeline_id}/requests/{request_id}'

payload = json.dumps({
  "storage": "dbfs:/pipelines",
  "libraries": [
    {
      "notebook": {
        "path": "/path/to/your/dlt-notebook"
      }
    }
  ]
})
headers={'Authorization': f'Bearer {host_token}', 'Content-Type':'application/json'}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)



"""

	7. Stop any Update/pipeline

"""

import requests
import json

url = f'https://{host_name}/api/2.0/pipelines/{pipeline_id}/stop'

payload = json.dumps({
  "name": "Wiki-Python-3",
  "storage": "dbfs:/pipelines",
  "libraries": [
    {
      "notebook": {
        "path": "/path/to/your/dlt-notebook"
      }
    }
  ]
})
headers={'Authorization': f'Bearer {host_token}', 'Content-Type':'application/json'}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)


"""

## Keypoints Observed While Testing Scripts


1. While Creating a pipeline, in the Payload one needs to mention the details one requires in the Payload.
  - For example - Photon Acceleration (Boolean), Else By Default it is false; Min and Max number of users.
2. One Cannot run multiple Pipeline on same notebook; Some changes/Add/Deletion of Tables need to be done.
  - IT will show an error that a particular Delta Table is managed by 'X-pipeline_ID'
3. On editing a pipeline, One Cannot edit a pipeline unless and until it is Created/Failed Successfully.
  - Example can be taken as a Adding Photon Acceleration/ Changing name of the pipeline.
4. Creating a pipeline Just creates one, to run a pipeline; AirFlow DAG/ Job needs to be configured which mentions time triggering, Retries after failures etc.


"""