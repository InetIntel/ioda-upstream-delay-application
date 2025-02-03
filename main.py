import logging
import json
import asyncio
import datetime
import os
import pathlib
import platform
import yaml

from multiprocessing import Pool

#from data_uploader.initialize import init as db_init
from data_uploader.post import post_data
import prober 


def setup():
    # set default config values, then load user-provided config file (if it exists)
    config_path = os.getenv("CONFIG_FILE")
    if not config_path:
        config_path = "/config/config.yaml"

    config = {"vp" : {},
              "prober" : {"tmp_dir" : "/data/tmp",
                          "tmp_output_file" : "/data/tmp/output.yrp",
                          "probe_rate" : 60000,
                          "interval" : 1800, # in seconds
                          "max_ttl" : 32,
                          "probe_type" : "ICMP",
                          "targets_file" : "/ioda-upstream-delay-application/source_data/test_ip",
                         },
              "reporting" : {},
              "logging" : {'path' : '/data/logging.log'}
             }
    try:
        with open(config_path, "r") as f:
            user_config = yaml.safe_load(f)
            config.update(user_config)
    except FileNotFoundError:
        # No config file exists, use default settings above
        pass

    # setup logging
    logging.basicConfig(
        filename=config['logging']['path'],
        filemode='a',
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    dirs_to_make = [os.path.dirname(config['prober']['tmp_output_file']),
                    os.path.dirname(config['logging']['path'])]
    for directory in dirs_to_make:
        pathlib.Path(directory).mkdir(parents=True, exist_ok=True)

    if "id" not in config['vp']:
        config['vp']['id'] = "asdf1234"

    logging.info(f"conf loaded")
    return config


def ceil_datetime(dt, dt_delta):
    return dt + (datetime.datetime.min - dt) % dt_delta

async def prober_loop(config):
    logging.info("IODA Upstream Delay Vantage Point starting...")
    logging.info("VP initialization...")
    #db_init(config)

    logging.info("VP initialized - fetch targets")
    #await check_and_update_target_list(conf)

    probe_interval = datetime.timedelta(seconds=config['prober']['interval'])
    while True:
        try:
            logging.info("Running prober")
            await prober.run(config)
            logging.info("Posting data")
            await post_data(config)
            curr = datetime.datetime.now()
            next_run = ceil_datetime(curr, probe_interval)
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
