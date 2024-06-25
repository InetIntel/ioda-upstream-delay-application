import asyncio
import os
import shutil
from datetime import datetime
import logging

async def post_data(conf):
    logging.info("Start data processing - fake posting")
    
    timestamp = datetime.now().strftime("%B-%d-%H-%M")
    
    src_path = conf["yarrp"]["intermediate_output_file"]
    dst_folder_path = os.path.join(conf["yarrp"]["result_folder"], timestamp)
    dst_path = os.path.join(dst_folder_path, f"{timestamp}-test.yrp")
    
    try:
        if not os.path.exists(src_path):
            logging.error(f"post data - src not found - {src_path}")
            raise FileNotFoundError(f"post data - src not found - {src_path}")
        
        if not os.path.exists(dst_folder_path):
            os.makedirs(dst_folder_path)
            logging.info(f"post data - create dst - {dst_folder_path}")
        
        shutil.move(src_path, dst_path)
        logging.info(f"post data - move {src_path} to {dst_path}")
    
    except Exception as e:
        logging.error(f"error in posting data: {e}")

