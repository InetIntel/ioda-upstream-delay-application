import uuid
import hashlib
import requests
from requests.auth import HTTPBasicAuth
import json

from elasticsearch import Elasticsearch


"""
Initialization should be done when setting up the server
"""

def generate_deterministic_uuid(prober_id, salt="x"):
    hash_value = hashlib.md5((prober_id + salt).encode()).hexdigest()
    return uuid.UUID(hash_value)

def create_index_in_es(es_url, vp_id, index_type, es_username=None, es_password=None, es_token=None):
    index_name = f"iupd-{index_type}-{vp_id}-002024"
    alias_name = f"iupd-{index_type}-{vp_id}"
    
    if index_type == "data":
        index_settings = {
            "aliases": {
                alias_name: {
                    "is_write_index": True
                }
            },
            "settings": {
                "number_of_shards": 5,
                "number_of_replicas": 1
            },
            "mappings": {
                "properties": {
                    "timestamp": { "type": "date" },
                    "as_number": { "type": "keyword" },
                    "prefixes": {
                        "type": "nested",
                        "properties": {
                            "prefix": { "type": "text" },
                            "latency": { "type": "float" },
                            "penultimate_as": { "type": "text" },
                            "full_tr_result": { "type": "text" }
                        }
                    }
                }
            }
        }
    elif index_type == "stat":
        index_settings = {
            "aliases": {
                alias_name: {
                    "is_write_index": True
                }
            },
            "settings": {
                "number_of_shards": 5,
                "number_of_replicas": 1
            },
            "mappings": {
                "properties": {
                    "timestamp": { "type": "date" },
                    "message": { "type": "text" },
                    "details": { "type": "object" }
                }
            }
        }
    else:
        raise ValueError(f"Unknown index type: {index_type}")

    if es_token != None:
        auth = None
        headers = {"Content-Type": "application/json",
                   "authorization" : "ApiKey "+es_token}
    elif es_username != None and es_password != None:
        headers = {"Content-Type": "application/json"}
        auth = (es_username, es_password)
    else:
        # Should not get here, TODO: make an error
        pass
        
    client = Elasticsearch("http://localhost:8000", api_key=("OcDUwd2GQG7hyua0dMkzn6dj3PdhRJWQREGB7pgpJpA"))
    response = requests.put(f"{es_url}/{index_name}", 
                            headers=headers,
                            auth=auth,
                            data=json.dumps(index_settings),
                            verify=False)
    
    if response.status_code not in [200, 201]:
        print(f"Failed to create index {index_name}: {response.text}")
    else:
        print(f"Successfully created index {index_name}")

def init(conf):
    vp_id = conf['vp']['id']
    for server in conf['reporting']:
        es_url = conf['REPORT_SERVER']
        #username = conf['es_username']
        #password = conf['es_password']
        es_token = conf['API_TOKEN']
        
        #create_index_in_es(es_url, username, password, uuid_str, "data")
        #create_index_in_es(es_url, username, password, uuid_str, "stat")
        create_index_in_es(es_url=es_url, es_token=es_token, vp_id=vp_id, index_type="stat")
