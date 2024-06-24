import json
import asyncio
import argparse
import logging

def load_config(file_path):
    with open(file_path, 'r') as file:
        return json.load(file) 

async def run_yarrp(config_file):
    config = load_config(config_file)
    yarrp_command = [
        'yarrp/yarrp',
        '-o', config['yarrp']['output_file'],
        '-i', config['yarrp']['target_file'],
        '-r', str(config['yarrp']['probe_rate']),
        '-t', config['yarrp']['probe_type'],
        '-v',
        '-m', str(config['yarrp']['max_ttl']),
    ]

    command = ' '.join(yarrp_command)
    logging.info(f"Executing command: {command}")
    process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await process.communicate()

    if process.returncode == 0:
        logging.info(f'Command succeeded: {stdout.decode()}')
    else:
        logging.info(f'Command failed: {stderr.decode()}')
