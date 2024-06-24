import cProfile
import logging
from multiprocessing import Pool
import json
import asyncio
import datetime
import time
import os

from probe.internet_scanner import run_yarrp
from data_posting.post import post_data

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
    

async def run_at_next_whole_hour(conf):
    await run_yarrp(conf)
    await post_data()
    
    while True:
        curr = datetime.datetime.now()
        next_whole_hour = curr.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
        delta = (next_whole_hour - curr).total_seconds()
        logging.debug(f"delta is {delta}")
        await asyncio.sleep(delta)

        await run_yarrp(conf)
        await post_data()

async def main():
    conf = "configuration/test_config.json"
    await run_at_next_whole_hour(conf)


if __name__ == "__main__":
    asyncio.run(main())
