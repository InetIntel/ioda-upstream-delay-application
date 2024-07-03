import cProfile
import logging
import subprocess
from multiprocessing import Pool
import json
import asyncio
import datetime
import time
import os
import platform

from probe.internet_scanner import run_yarrp
from data_posting.post import post_data
from target_generation.census_analysis import address_analysis
from target_generation.generate_target import address_generation

def setup():
    # setup logging
    logging.basicConfig(
        filename='logging.log',
        filemode='a',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

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

    # setup configuration - APP_ENV needs to be set prior the running
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

    env_config = configs.get(env, configs['default'])
    logging.info(f"conf loaded - {env}")

    return env_config

    

async def run_at_next_whole_hour(conf):
    await run_yarrp(conf)
    await post_data(conf)
    
    while True:
        curr = datetime.datetime.now()
        next_whole_hour = curr.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
        delta = (next_whole_hour - curr).total_seconds()
        logging.info(f"delta is {delta}")
        await asyncio.sleep(delta)

        await run_yarrp(conf)
        await post_data(conf)



async def main():
    env_config = setup()
    await run_at_next_whole_hour(env_config)
    #address_analysis()
    #address_generation()
    

if __name__ == "__main__":
    asyncio.run(main())
