import os
import shutil
from datetime import datetime
import logging
import requests
import json
from concurrent.futures import ThreadPoolExecutor
import time

import parser





# Every file is responsible for an AS, and thus a document in ES
def process_file(file_path, folder_name):
    file_name = os.path.basename(file_path)
    as_number = file_name.split('.')[0]

    try:
        timestamp = datetime.strptime(folder_name, '%Y-%m-%dT%H-%M').isoformat()
    except ValueError as e:
        logging.info(f"Failed parsing timestamp - {folder_name}")
        return None
    
    with open(file_path, 'r') as f:
        lines = f.readlines()
        if not lines:
            logging.info(f"No data in file - {folder_name} - {file_path}")
            return None
        
        return [json.loads(line.strip()) for line in lines]

def create_bulk_data(documents, vpid):
    bulk_data = []
    for doc in documents:
        bulk_data.append(json.dumps({"index": {"_index": f"iupd-data-{vpid}"}}))
        bulk_data.append(json.dumps(doc))
    return "\n".join(bulk_data) + "\n"

def post_bulk_data(bulk_data, reporting_server):
    es_url = reporting_server['url']
    authentication = reporting_server['authentication']
    if authentication['method'] == 'user':
        auth = (authentication['user'],authentication['password'])
        headers = {"Content-Type": "application/x-ndjson"}
    elif authentication['method'] == 'ApiKey':
        auth = None
        headers = {"Content-Type": "application/x-ndjson",
                   "authorization" : "ApiKey "+authentication['token']}

    try:
        response = requests.post(
            es_url+"/_bulk",
            headers=headers,
            data=bulk_data,
            auth=auth,
            verify=False
        )
        if response.status_code not in [200, 201]:
            logging.error(f"Failed to insert documents: {response.text}")
    except requests.RequestException as e:
        logging.error(f"Request exception: {e}")

# Multi processing + bulk API (for ES to efficiently index documents)
async def process_directory(folder_name, vpid, reporting_server, batch_size=100, num_threads=5):
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = []
        logging.info(f"Processing directory - {folder_name}")
        start_time = time.time()
        documents = []
        for file in os.listdir(folder_name):
            if file.endswith('.json'):
                file_path = os.path.join(folder_name, file)
                docs = process_file(file_path, folder_name.split("/")[-2])
                if len(docs) > 0:
                    documents += docs
                    if len(documents) >= batch_size:
                        bulk_data = create_bulk_data(documents, vpid)
                        futures.append(executor.submit(post_bulk_data, bulk_data, reporting_server))
                        documents = [] 
        
        if len(documents) > 0:  
            bulk_data = create_bulk_data(documents, vpid)
            futures.append(executor.submit(post_bulk_data, bulk_data, reporting_server))
    
        for future in futures:
            future.result()

        end_time = time.time() 
        duration = end_time - start_time
        logging.info(f"Post stage - {duration:.2f} seconds - {folder_name}")



async def post_data(config):
    logging.info("Start data processing")

    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M")

    src_path = config["prober"]["tmp_output_file"]
    dst_folder_path = os.path.join(config["prober"]['tmp_dir'], timestamp)
    dst_path = os.path.join(dst_folder_path, f"{timestamp}.yrp")
    uuid_str = config['vp']['id']
    
    try:
        if not os.path.exists(src_path):
            logging.error(f"post data - src not found - {src_path}")
            raise FileNotFoundError(f"post data - src not found - {src_path}")

        if not os.path.exists(dst_folder_path):
            os.makedirs(dst_folder_path)
            logging.info(f"post data - created destination folder - {dst_folder_path}")

        shutil.move(src_path, dst_path)
        parser.process_yarrp_result(dst_folder_path, f"{timestamp}.yrp")

        for reporting_server in config['reporting']:
            logging.info(f"Processing data for server: "+reporting_server)
            await process_directory(os.path.join(dst_folder_path, "result"), uuid_str, config['reporting'][reporting_server])

        logging.info("post data finished")

    except Exception as e:
        logging.error(f"Error in posting data: {e}")
