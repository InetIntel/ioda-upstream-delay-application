import uuid
import hashlib
import requests
import json

"""
Initialization should be done when setting up the server
"""

def generate_deterministic_uuid(prober_id, salt="x"):
    hash_value = hashlib.md5((prober_id + salt).encode()).hexdigest()
    return uuid.UUID(hash_value)

def create_index_in_es(es_url, username, password, uuid_str, index_type):
    index_name = f"iupd-{index_type}-{uuid_str}-002024"
    alias_name = f"iupd-{index_type}-{uuid_str}"
    
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
    
    response = requests.put(f"{es_url}/{index_name}", 
                            headers={"Content-Type": "application/json"}, 
                            auth=(username, password), 
                            data=json.dumps(index_settings),
                            verify=False)
    
    if response.status_code not in [200, 201]:
        print(f"Failed to create index {index_name}: {response.text}")
    else:
        print(f"Successfully created index {index_name}")

def init(conf):
    uuid_str = conf['uuid']
    es_url = conf['es_url']
    username = conf['es_username']
    password = conf['es_password']
    
    create_index_in_es(es_url, username, password, uuid_str, "data")
    #create_index_in_es(es_url, username, password, uuid_str, "stat")
