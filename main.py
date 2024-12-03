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
    required_app_vars = ['REMOTE_STORAGES', 'REMOTE_STORAGES_USERS', 'REMOTE_STORAGES_PASSWORDS', 
                         'UUID', 'TARGET_FILE', 'INTERMEDIATE_OUTPUT_FILE', 'RESULT_FOLDER', 
                         'PROBE_RATE', 'MAX_TTL']
    
    # List of optional environmental variables and their default values
    optional_app_vars = {'INTERVAL' : 1800 # Interval (in seconds) between running measurements
                         }
    app_es_config = {}

    for var in required_app_vars:
        value = os.getenv(var)
        if not value:
            logging.error(f"Environment variable {var} is required but not set.")
            raise EnvironmentError(f"Environment variable {var} is required but not set.")
        app_es_config[var] = value

    for var in optional_app_vars:
        value = os.getenv(var)
        if not value:
            value = optional_app_vars[var]
        app_es_config[var] = value
    
    logging.info(f"conf loaded")
    return app_es_config


def ceil_datetime(dt, dt_delta):
    return dt + (datetime.datetime.min - dt) % dt_delta

async def prober_loop(conf):
    logging.info(f"VP initialized started")
    db_init(conf)

    logging.info(f"VP initialized - fetch targets")
    #await check_and_update_target_list(conf)

    probe_interval = datetime.timedelta(seconds=conf['INTERVAL'])
    # if post_data is not successful, then record
    while True:
        try:
            await run_yarrp(conf)
            await post_data(conf)
            curr = datetime.datetime.now()
            next_run = ceil_datetime(curr,probe_interval)
            sleep_time = (next_run - curr).total_seconds()
            logging.info(f"Sleeping for {sleep_time} sec")
            await asyncio.sleep(sleep_time)
        except Exception as e:
            logging.error(f"error duing  - {e}")
        # await check_and_update_target_list(conf)

async def main():
    config = setup()
    probing_task = asyncio.create_task(prober_loop(config))
    await asyncio.gather(probing_task)
    
if __name__ == "__main__":
    asyncio.run(main())
