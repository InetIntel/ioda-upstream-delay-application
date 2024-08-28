import logging
from multiprocessing import Pool
import json
import asyncio
import datetime
import os
import platform

from data_posting.initialize import init as db_init
from data_posting.post import post_data

from prober.internet_scanner import run_yarrp


def setup():
    # setup logging
    logging.basicConfig(
        filename='logging.log',
        filemode='a',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # validate env variables
    app_vars = ['es_url', 'es_username', 'es_password', 'uuid']
    app_es_config = {}
    for var in app_vars:
        value = os.getenv(var.upper())
        if not value:
            logging.error(f"Environment variable {var} is required but not set.")
            raise EnvironmentError(f"Environment variable {var} is required but not set.")
        app_es_config[var] = value

    # setup env
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

    # setup configuration - APP_ENV needs to be set or we use default as docker
    if os.getenv('APP_ENV') not in ['docker', 'macos', 'ubuntu']:
        if os.path.exists('/.dockerenv'):
            env = 'docker'
        elif platform.system() == 'Darwin':
            env = 'macos'
        elif platform.system() == 'Linux':
            env = 'ubuntu'
    else:
        env = 'docker'
    
    with open('configuration/config.json', 'r') as f:
        configs = json.load(f)


    # combine config for iupd and config for es
    combined_config = {**configs.get(env, configs['default']), **app_es_config}
    logging.info(f"conf loaded - {env}")

    return combined_config

    

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
 