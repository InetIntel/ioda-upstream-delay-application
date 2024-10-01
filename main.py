import logging
from multiprocessing import Pool
import json
import asyncio
import datetime
import os
import platform

from data_uploader.initialize import init as db_init
from data_uploader.post import post_data

from internet_prober.internet_scanner import run_yarrp


def setup():
    # setup logging
    logging.basicConfig(
        filename='logging.log',
        filemode='a',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # setup dirs
    directories = [
        'result',
        'intermediate_result'
    ]
    
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory, True)
            logging.info(f"create directory - {directory}")
        else:
            logging.info(f"directory existed - {directory}")

    logging.info("basic setup complete")


    # validate env variables
    app_vars = ['REMOTE_STORAGES', 'REMOTE_STORAGES_USERS', 'REMOTE_STORAGES_PASSWORDS', 'UUID', 'TARGET_FILE', 'INTERMEDIATE_OUTPUT_FILE', 'RESULT_FOLDER', 'PROBE_RATE', 'MAX_TTL']
    app_es_config = {}
    for var in app_vars:
        value = os.getenv(var)
        if not value:
            logging.error(f"Environment variable {var} is required but not set.")
            raise EnvironmentError(f"Environment variable {var} is required but not set.")
        app_es_config[var] = value
    
    logging.info(f"conf loaded")
    return app_es_config

    

async def run_at_every_whole_hour(conf):
    logging.info(f"VP initialized started")
    db_init(conf)

    logging.info(f"VP initialized - fetch targets")
    #await check_and_update_target_list(conf)

    # if post_data is not successful, then record
    try:
        await run_yarrp(conf)
        await post_data(conf)
    except Exception as e:
        logging.errir(f"error duing first run - {e}")

    while True:
        try:
            curr = datetime.datetime.now()
            time_of_next_whole_hour = curr.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
            delta = (time_of_next_whole_hour - curr).total_seconds()
            logging.info(f"time left - {delta} sec")
            await asyncio.sleep(delta)

            await run_yarrp(conf)
            await post_data(conf)
        except Exception as e:
            logging.errir(f"error duing  - {e}")
        
        # await check_and_update_target_list(conf)



async def main():
    config = setup()
    probing_task = asyncio.create_task(run_at_every_whole_hour(config))
    await asyncio.gather(probing_task)
    
    

if __name__ == "__main__":
    asyncio.run(main())
 