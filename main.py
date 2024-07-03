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
import uvicorn


from probe.internet_scanner import run_yarrp
from data_posting.post import post_data
#from target_generation.census_analysis import address_analysis
#from target_generation.generate_target import address_generation
from webhook.hook import app as test

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
    logging.info(f"run_at_next_whole_hour entered")
    return
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


async def run_hook_server():
    # TODO: adjust endpoint
    config = uvicorn.Config(app=test, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    env_config = setup()
    uvicorn_task = asyncio.create_task(run_hook_server())
    probing_task = asyncio.create_task(run_at_next_whole_hour(env_config))
    await asyncio.gather(uvicorn_task, probing_task)
    
    # On server
    # address_analysis()
    # address_generation()
    

if __name__ == "__main__":
    asyncio.run(main())
